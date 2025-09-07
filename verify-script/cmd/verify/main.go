package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"time"

	"github.com/cosmos/cosmos-sdk/client"
	"github.com/cosmos/cosmos-sdk/client/tx"
	sdk "github.com/cosmos/cosmos-sdk/types"
	"github.com/cosmos/cosmos-sdk/types/query"
	gc "github.com/gitopia/gitopia-go"
	"github.com/gitopia/gitopia-go/logger"
	"github.com/gitopia/gitopia-migration-tools/shared"
	gitopiatypes "github.com/gitopia/gitopia/v6/x/gitopia/types"
	ipfsclusterclient "github.com/ipfs-cluster/ipfs-cluster/api/rest/client"
	"github.com/pkg/errors"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

const (
	AccountAddressPrefix = "gitopia"
	AccountPubKeyPrefix  = AccountAddressPrefix + sdk.PrefixPublic
	AppName              = "gitopia-verify-script"
	VerificationLogFile  = "verification_failures.json"
)

type VerificationFailure struct {
	RepositoryID uint64 `json:"repository_id"`
	RefName      string `json:"ref_name"`
	RefSHA       string `json:"ref_sha"`
	Error        string `json:"error"`
	Timestamp    string `json:"timestamp"`
}

type VerificationLog struct {
	Failures []VerificationFailure `json:"failures"`
	mu       sync.Mutex            // Mutex for thread-safe access
}

// ParentRepoManager manages parent repository setup with race condition protection
type ParentRepoManager struct {
	setupMutexes map[uint64]*sync.Mutex // Per-parent-repo mutexes
	refCounts    map[uint64]int         // Reference counts for cleanup
	globalMutex  sync.Mutex             // Protects the maps above
}

func NewParentRepoManager() *ParentRepoManager {
	return &ParentRepoManager{
		setupMutexes: make(map[uint64]*sync.Mutex),
		refCounts:    make(map[uint64]int),
	}
}

// getOrCreateMutex returns the mutex for a parent repo, creating it if necessary
func (prm *ParentRepoManager) getOrCreateMutex(parentRepoID uint64) *sync.Mutex {
	prm.globalMutex.Lock()
	defer prm.globalMutex.Unlock()

	if mutex, exists := prm.setupMutexes[parentRepoID]; exists {
		return mutex
	}

	mutex := &sync.Mutex{}
	prm.setupMutexes[parentRepoID] = mutex
	return mutex
}

// acquireParentRepo safely sets up a parent repository and increments reference count
func (prm *ParentRepoManager) acquireParentRepo(ctx context.Context, parentRepoID uint64, tempDir string, storageManager *shared.StorageManager) (string, error) {
	// Get the mutex for this specific parent repo
	mutex := prm.getOrCreateMutex(parentRepoID)
	mutex.Lock()
	defer mutex.Unlock()

	parentRepoDir := filepath.Join(tempDir, fmt.Sprintf("parent_%d.git", parentRepoID))

	// Increment reference count
	prm.globalMutex.Lock()
	prm.refCounts[parentRepoID]++
	refCount := prm.refCounts[parentRepoID]
	prm.globalMutex.Unlock()

	// If directory already exists, just return it
	if _, err := os.Stat(parentRepoDir); err == nil {
		fmt.Printf("Reusing existing parent repository directory for parent %d\n", parentRepoID)
		return parentRepoDir, nil
	}

	// Setup the parent repository (only first worker will do this)
	if refCount == 1 {
		if err := prm.setupParentRepoInternal(ctx, parentRepoID, parentRepoDir, tempDir, storageManager); err != nil {
			// Decrement ref count on failure
			prm.globalMutex.Lock()
			prm.refCounts[parentRepoID]--
			if prm.refCounts[parentRepoID] == 0 {
				delete(prm.refCounts, parentRepoID)
			}
			prm.globalMutex.Unlock()
			return "", err
		}
	}

	return parentRepoDir, nil
}

// releaseParentRepo decrements reference count (parent repo persists for reuse)
func (prm *ParentRepoManager) releaseParentRepo(parentRepoID uint64, parentRepoDir string) {
	prm.globalMutex.Lock()
	defer prm.globalMutex.Unlock()

	prm.refCounts[parentRepoID]--
	if prm.refCounts[parentRepoID] <= 0 {
		delete(prm.refCounts, parentRepoID)
		// Parent repository directory is kept for future reuse
	}
}

// setupParentRepoInternal does the actual parent repository setup
func (prm *ParentRepoManager) setupParentRepoInternal(ctx context.Context, parentRepoID uint64, parentRepoDir, tempDir string, storageManager *shared.StorageManager) error {
	// Get parent repository packfile info from database
	parentPackfileInfo := storageManager.GetPackfileInfo(parentRepoID)
	if parentPackfileInfo == nil {
		return errors.Errorf("parent repository %d packfile not found in database", parentRepoID)
	}

	// Create parent repository directory
	if err := os.MkdirAll(parentRepoDir, 0755); err != nil {
		return errors.Wrap(err, "failed to create parent repository directory")
	}

	// Initialize bare repository
	initCmd := exec.CommandContext(ctx, "git", "init", "--bare", parentRepoDir)
	if output, err := initCmd.CombinedOutput(); err != nil {
		return errors.Wrapf(err, "failed to initialize parent repository: %s", string(output))
	}

	// Create parent pack directory
	parentPackDir := filepath.Join(parentRepoDir, "objects", "pack")
	if err := os.MkdirAll(parentPackDir, 0755); err != nil {
		return errors.Wrap(err, "failed to create parent pack directory")
	}

	// Fetch parent packfile directly to pack directory
	parentPackfilePath, err := fetchPackfileFromIPFS(ctx, parentPackfileInfo.CID, parentPackfileInfo.Name, parentPackDir)
	if err != nil {
		return errors.Wrapf(err, "failed to fetch parent packfile from IPFS")
	}

	// Import packfile into parent repository
	indexPackCmd := exec.CommandContext(ctx, "git", "index-pack", parentPackfilePath)
	indexPackCmd.Dir = parentPackDir
	if output, err := indexPackCmd.CombinedOutput(); err != nil {
		return errors.Wrapf(err, "failed to index parent packfile: %s", string(output))
	}

	return nil
}

func loadVerificationLog() (*VerificationLog, error) {
	if _, err := os.Stat(VerificationLogFile); os.IsNotExist(err) {
		return &VerificationLog{
			Failures: make([]VerificationFailure, 0),
		}, nil
	}

	data, err := os.ReadFile(VerificationLogFile)
	if err != nil {
		return nil, err
	}

	var log VerificationLog
	if err := json.Unmarshal(data, &log); err != nil {
		return nil, err
	}

	return &log, nil
}

func saveVerificationLog(log *VerificationLog) error {
	log.mu.Lock()
	defer log.mu.Unlock()
	data, err := json.Marshal(log)
	if err != nil {
		return errors.Wrap(err, "failed to marshal verification log")
	}
	return os.WriteFile(VerificationLogFile, data, 0644)
}

func logVerificationFailure(log *VerificationLog, repoID uint64, refName, refSHA, errorMsg string) {
	failure := VerificationFailure{
		RepositoryID: repoID,
		RefName:      refName,
		RefSHA:       refSHA,
		Error:        errorMsg,
		Timestamp:    time.Now().UTC().Format(time.RFC3339),
	}
	log.mu.Lock()
	log.Failures = append(log.Failures, failure)
	log.mu.Unlock()
}

// fileExists checks if a file exists
func fileExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}

// fetchPackfileFromIPFS downloads a packfile from IPFS with efficient handling for large files
func fetchPackfileFromIPFS(ctx context.Context, cid, packfileName, targetDir string) (string, error) {
	// Create packfile directly in target directory
	packfilePath := filepath.Join(targetDir, packfileName)

	// Use IPFS HTTP API to fetch the file with streaming
	ipfsURL := fmt.Sprintf("http://%s:%s/api/v0/cat?arg=%s",
		viper.GetString("IPFS_HOST"),
		viper.GetString("IPFS_PORT"),
		cid)

	req, err := http.NewRequestWithContext(ctx, "POST", ipfsURL, nil)
	if err != nil {
		return "", errors.Wrap(err, "failed to create IPFS request")
	}

	client := &http.Client{
		Timeout: 30 * time.Minute, // Allow up to 30 minutes for large files
	}

	resp, err := client.Do(req)
	if err != nil {
		return "", errors.Wrap(err, "failed to fetch from IPFS")
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return "", errors.Errorf("IPFS returned status %d", resp.StatusCode)
	}

	// Create the output file
	outFile, err := os.Create(packfilePath)
	if err != nil {
		return "", errors.Wrap(err, "failed to create packfile")
	}
	defer outFile.Close()

	// Stream the content to avoid memory issues with large files
	_, err = io.Copy(outFile, resp.Body)
	if err != nil {
		return "", errors.Wrap(err, "failed to write packfile")
	}

	return packfilePath, nil
}

// VerificationJob represents a repository verification job
type VerificationJob struct {
	RepoID uint64
}

// VerificationResult represents the result of a verification job
type VerificationResult struct {
	RepoID uint64
	Error  error
}

// verifyRepository checks if all refs exist in the repository packfile
func verifyRepository(ctx context.Context, repoID uint64, gitopiaClient *gc.Client, storageManager *shared.StorageManager, tempDir string, log *VerificationLog, parentRepoManager *ParentRepoManager) error {
	// Skip repo 62771 due to 30GB size
	if repoID == 62771 {
		fmt.Printf("Skipping repository %d (30GB binary files)\n", repoID)
		return nil
	}

	// Get repository info from Gitopia
	repo, err := gitopiaClient.QueryClient().Gitopia.Repository(ctx, &gitopiatypes.QueryGetRepositoryRequest{
		Id: repoID,
	})
	if err != nil {
		// Repository might not exist, skip silently
		return nil
	}

	// Query branches early to skip repositories with no branches
	branchAllRes, err := gitopiaClient.QueryClient().Gitopia.RepositoryBranchAll(ctx, &gitopiatypes.QueryAllRepositoryBranchRequest{
		Id:             repo.Repository.Owner.Id,
		RepositoryName: repo.Repository.Name,
		Pagination: &query.PageRequest{
			Limit: math.MaxUint64,
		},
	})
	if err != nil {
		logVerificationFailure(log, repoID, "", "", fmt.Sprintf("failed to query branches: %v", err))
		return nil
	}

	// Skip repositories with no branches
	if len(branchAllRes.Branch) == 0 {
		return nil
	}

	// Create temporary repository directory (reuse if exists)
	repoDir := filepath.Join(tempDir, fmt.Sprintf("verify_%d.git", repoID))
	repoExists := false
	if _, err := os.Stat(repoDir); err == nil {
		repoExists = true
		fmt.Printf("Reusing existing repository directory for repo %d\n", repoID)
	} else {
		if err := os.MkdirAll(repoDir, 0755); err != nil {
			return errors.Wrap(err, "failed to create repository directory")
		}
		// Initialize bare repository only if it doesn't exist
		initCmd := exec.CommandContext(ctx, "git", "init", "--bare", repoDir)
		if output, err := initCmd.CombinedOutput(); err != nil {
			return errors.Wrapf(err, "failed to initialize repository: %s", string(output))
		}
	}
	// defer os.RemoveAll(repoDir)

	// Handle fork repositories with alternates
	var parentRepoDir string
	var needsParentCleanup bool
	if repo.Repository.Fork {
		parentRepoDir, err = parentRepoManager.acquireParentRepo(ctx, repo.Repository.Parent, tempDir, storageManager)
		if err != nil {
			logVerificationFailure(log, repoID, "", "", fmt.Sprintf("failed to setup parent repository: %v", err))
			return nil
		}
		needsParentCleanup = true
		defer func() {
			if needsParentCleanup {
				parentRepoManager.releaseParentRepo(repo.Repository.Parent, parentRepoDir)
			}
		}()

		// Set up alternates file (only if repo doesn't exist or alternates file is missing)
		alternatesPath := filepath.Join(repoDir, "objects", "info", "alternates")
		if !repoExists || !fileExists(alternatesPath) {
			if err := os.MkdirAll(filepath.Dir(alternatesPath), 0755); err != nil {
				return errors.Wrap(err, "failed to create alternates directory")
			}
			parentObjectsPath := filepath.Join(parentRepoDir, "objects")
			if err := os.WriteFile(alternatesPath, []byte(parentObjectsPath+"\n"), 0644); err != nil {
				return errors.Wrap(err, "failed to create alternates file")
			}
		}
	}

	// Get packfile info from database
	packfileInfo := storageManager.GetPackfileInfo(repoID)

	// Handle case where fork repositories have no packfile of their own
	if packfileInfo == nil {
		if repo.Repository.Fork {
			// For fork repositories, it's expected that some may have no packfile
			// They rely entirely on parent repository objects via alternates
			fmt.Printf("Fork repository %d has no packfile (uses parent objects only)\n", repoID)
		} else {
			// For non-fork repositories, missing packfile is an error
			logVerificationFailure(log, repoID, "", "", "packfile not found in database")
			return nil
		}
	} else {
		// Create pack directory and fetch packfile if it exists
		packDir := filepath.Join(repoDir, "objects", "pack")
		if err := os.MkdirAll(packDir, 0755); err != nil {
			return errors.Wrap(err, "failed to create pack directory")
		}

		// Check if packfile already exists to avoid re-fetching
		packfilePath := filepath.Join(packDir, packfileInfo.Name)
		packfileExists := fileExists(packfilePath)

		if !packfileExists {
			// Fetch packfile directly to pack directory
			packfilePath, err = fetchPackfileFromIPFS(ctx, packfileInfo.CID, packfileInfo.Name, packDir)
			if err != nil {
				logVerificationFailure(log, repoID, "", "", fmt.Sprintf("failed to fetch packfile from IPFS: %v", err))
				return nil
			}
		} else {
			fmt.Printf("Reusing existing packfile for repo %d\n", repoID)
		}

		// Check if packfile is already indexed (look for .idx file)
		idxPath := packfilePath[:len(packfilePath)-5] + ".idx" // Replace .pack with .idx
		if !fileExists(idxPath) {
			indexPackCmd := exec.CommandContext(ctx, "git", "index-pack", packfilePath)
			indexPackCmd.Dir = packDir
			if output, err := indexPackCmd.CombinedOutput(); err != nil {
				logVerificationFailure(log, repoID, "", "", fmt.Sprintf("failed to index packfile: %v, output: %s", err, string(output)))
				return nil
			}
		}
	}

	// Query tags from Gitopia
	tagAllRes, err := gitopiaClient.QueryClient().Gitopia.RepositoryTagAll(ctx, &gitopiatypes.QueryAllRepositoryTagRequest{
		Id:             repo.Repository.Owner.Id,
		RepositoryName: repo.Repository.Name,
		Pagination: &query.PageRequest{
			Limit: math.MaxUint64,
		},
	})
	if err != nil {
		logVerificationFailure(log, repoID, "", "", fmt.Sprintf("failed to query tags: %v", err))
		return nil
	}

	// Verify all branches
	for _, branch := range branchAllRes.Branch {
		catFileCmd := exec.CommandContext(ctx, "git", "cat-file", "-e", branch.Sha)
		catFileCmd.Dir = repoDir
		if err := catFileCmd.Run(); err != nil {
			logVerificationFailure(log, repoID, fmt.Sprintf("refs/heads/%s", branch.Name), branch.Sha, "object not found in packfile")
		}
	}

	// Verify all tags
	for _, tag := range tagAllRes.Tag {
		catFileCmd := exec.CommandContext(ctx, "git", "cat-file", "-e", tag.Sha)
		catFileCmd.Dir = repoDir
		if err := catFileCmd.Run(); err != nil {
			logVerificationFailure(log, repoID, fmt.Sprintf("refs/tags/%s", tag.Name), tag.Sha, "object not found in packfile")
		}
	}

	return nil
}

// retryFailedRepositories processes repositories that failed verification by cloning and pinning them
func retryFailedRepositories(ctx context.Context, gitopiaClient *gc.Client, storageManager *shared.StorageManager,
	ipfsClusterClient ipfsclusterclient.Client, pinataClient *shared.PinataClient, tempDir string,
	parentRepoManager *ParentRepoManager, log *VerificationLog) error {

	// Load current verification failures
	if len(log.Failures) == 0 {
		fmt.Println("No verification failures found to retry")
		return nil
	}

	// Extract unique repository IDs from failures
	failedRepoIDs := make(map[uint64]bool)
	for _, failure := range log.Failures {
		if failure.Error == "packfile not found in database" {
			failedRepoIDs[failure.RepositoryID] = true
		}
	}

	fmt.Printf("Found %d unique repositories with 'packfile not found in database' errors to retry\n", len(failedRepoIDs))

	successCount := 0
	for repoID := range failedRepoIDs {
		fmt.Printf("Retrying repository %d...\n", repoID)

		// Get repository info from Gitopia
		repo, err := gitopiaClient.QueryClient().Gitopia.Repository(ctx, &gitopiatypes.QueryGetRepositoryRequest{
			Id: repoID,
		})
		if err != nil {
			fmt.Printf("Repository %d not found, skipping: %v\n", repoID, err)
			continue
		}

		// Check if repository is empty
		_, err = gitopiaClient.QueryClient().Gitopia.RepositoryBranch(ctx, &gitopiatypes.QueryGetRepositoryBranchRequest{
			Id:             repo.Repository.Owner.Id,
			RepositoryName: repo.Repository.Name,
			BranchName:     repo.Repository.DefaultBranch,
		})
		if err != nil {
			fmt.Printf("Repository %d is empty, skipping\n", repoID)
			continue
		}

		// Clone and process the repository
		if err := cloneAndProcessRepository(ctx, repo.Repository, tempDir, ipfsClusterClient, storageManager, gitopiaClient, pinataClient, parentRepoManager); err != nil {
			fmt.Printf("Failed to clone and process repository %d: %v\n", repoID, err)
			continue
		}

		// Remove all failures for this repository from the log
		removeRepositoryFromFailures(log, repoID)
		successCount++
		fmt.Printf("Successfully processed repository %d\n", repoID)
	}

	fmt.Printf("Retry complete: %d repositories successfully processed\n", successCount)
	return nil
}

// removeRepositoryFromFailures removes all failure entries for a specific repository
func removeRepositoryFromFailures(log *VerificationLog, repoID uint64) {
	log.mu.Lock()
	defer log.mu.Unlock()

	newFailures := make([]VerificationFailure, 0)
	for _, failure := range log.Failures {
		if failure.RepositoryID != repoID {
			newFailures = append(newFailures, failure)
		}
	}
	log.Failures = newFailures
}

// cloneAndProcessRepository clones a repository and processes it like the clone script
func cloneAndProcessRepository(ctx context.Context, repository *gitopiatypes.Repository, tempDir string,
	ipfsClusterClient ipfsclusterclient.Client, storageManager *shared.StorageManager,
	gitopiaClient *gc.Client, pinataClient *shared.PinataClient, parentRepoManager *ParentRepoManager) error {

	repoID := repository.Id
	repoDir := filepath.Join(tempDir, fmt.Sprintf("retry_%d.git", repoID))
	remoteUrl := fmt.Sprintf("gitopia://%s/%s", repository.Owner.Id, repository.Name)

	// Clean up any existing directory
	if _, err := os.Stat(repoDir); err == nil {
		if err := os.RemoveAll(repoDir); err != nil {
			return errors.Wrapf(err, "failed to remove existing directory %s", repoDir)
		}
	}

	// Handle forked repository optimization
	if repository.Fork {
		fmt.Printf("Repository %d is a fork of %d, setting up optimized clone with alternates\n", repoID, repository.Parent)

		// Ensure parent repository is available
		parentRepoDir, err := parentRepoManager.acquireParentRepo(ctx, repository.Parent, tempDir, storageManager)
		if err != nil {
			return errors.Wrapf(err, "failed to setup parent repository %d for fork %d", repository.Parent, repoID)
		}
		defer parentRepoManager.releaseParentRepo(repository.Parent, parentRepoDir)

		// Clone fork with alternates optimization
		if err := cloneForkWithAlternates(ctx, remoteUrl, repoDir, parentRepoDir, repository.Parent); err != nil {
			return errors.Wrapf(err, "error cloning forked repository %d with alternates", repoID)
		}
	} else {
		// Standard clone for non-forked repositories
		cloneCtx, cancel := context.WithTimeout(ctx, 30*time.Minute)
		defer cancel()

		cmd := exec.CommandContext(cloneCtx, "git", "clone", "--bare", "--mirror", remoteUrl, repoDir)
		if output, err := cmd.CombinedOutput(); err != nil {
			return errors.Wrapf(err, "error cloning repository %d: %s", repoID, string(output))
		}
	}

	// Run git gc
	gcCtx, cancel := context.WithTimeout(ctx, 10*time.Minute)
	defer cancel()

	gcCmd := exec.CommandContext(gcCtx, "git", "gc")
	gcCmd.Dir = repoDir
	if output, err := gcCmd.CombinedOutput(); err != nil {
		return errors.Wrapf(err, "error running git gc for repo %d: %s", repoID, string(output))
	}

	// For forked repos with alternates, run git repack to remove common objects
	if repository.Fork {
		alternatesPath := filepath.Join(repoDir, "objects", "info", "alternates")
		if _, err := os.Stat(alternatesPath); err == nil {
			fmt.Printf("Running git repack to optimize forked repository %d\n", repoID)

			repackCtx, cancel := context.WithTimeout(ctx, 15*time.Minute)
			defer cancel()

			repackCmd := exec.CommandContext(repackCtx, "git", "repack", "-a", "-d", "-l")
			repackCmd.Dir = repoDir
			if output, err := repackCmd.CombinedOutput(); err != nil {
				fmt.Printf("Warning: git repack failed for forked repo %d: %v\nOutput: %s\n", repoID, err, string(output))
			}
		}
	}

	// Pin packfile to IPFS cluster
	packDir := filepath.Join(repoDir, "objects", "pack")
	packFiles, err := filepath.Glob(filepath.Join(packDir, "*.pack"))
	if err != nil {
		return errors.Wrapf(err, "error finding packfiles for repo %d", repoID)
	}

	if len(packFiles) > 0 {
		packfileName := packFiles[0]
		cid, err := shared.PinFile(ipfsClusterClient, packfileName)
		if err != nil {
			return errors.Wrapf(err, "error pinning packfile for repo %d", repoID)
		}

		rootHash, size, err := shared.ComputeFileInfo(packfileName, cid)
		if err != nil {
			return errors.Wrapf(err, "error computing file info for packfile repo %d", repoID)
		}

		packfileInfo := &shared.PackfileInfo{
			RepositoryID: repoID,
			Name:         filepath.Base(packfileName),
			CID:          cid,
			RootHash:     rootHash,
			Size:         size,
			UpdatedAt:    time.Now(),
			UpdatedBy:    "verify-retry",
		}
		storageManager.SetPackfileInfo(packfileInfo)
		fmt.Printf("Successfully pinned packfile for repository %d (CID: %s)\n", repoID, cid)

		// Pin to Pinata if enabled
		if pinataClient != nil {
			resp, err := pinataClient.PinFile(ctx, packfileName, filepath.Base(packfileName))
			if err != nil {
				fmt.Printf("Warning: failed to pin packfile to Pinata for repo %d: %v\n", repoID, err)
			} else {
				fmt.Printf("Successfully pinned packfile to Pinata for repository %d (Pinata ID: %s)\n", repoID, resp.Data.ID)
			}
		}
	} else if repository.Fork {
		fmt.Printf("Forked repository %d has no packfile (no new changes), using parent objects via alternates\n", repoID)
	}

	// Clean up repository directory
	if err := os.RemoveAll(repoDir); err != nil {
		fmt.Printf("Warning: failed to delete repository directory %s: %v\n", repoDir, err)
	}

	// Save storage manager state
	if err := storageManager.Save(); err != nil {
		return errors.Wrap(err, "failed to save storage manager")
	}

	return nil
}

// cloneForkWithAlternates performs an optimized clone of a forked repository using Git alternates
func cloneForkWithAlternates(ctx context.Context, remoteUrl, repoDir, parentRepoDir string, parentRepoID uint64) error {
	// Create the repository directory structure
	if err := os.MkdirAll(repoDir, 0755); err != nil {
		return errors.Wrap(err, "failed to create repository directory")
	}

	// Initialize bare repository
	initCtx, cancel := context.WithTimeout(ctx, 5*time.Minute)
	defer cancel()

	initCmd := exec.CommandContext(initCtx, "git", "init", "--bare", repoDir)
	if output, err := initCmd.CombinedOutput(); err != nil {
		return errors.Wrapf(err, "failed to initialize bare repository: %s", string(output))
	}

	// Set up alternates file BEFORE adding remote and fetching
	if _, err := os.Stat(parentRepoDir); err == nil {
		alternatesPath := filepath.Join(repoDir, "objects", "info", "alternates")
		if err := os.MkdirAll(filepath.Dir(alternatesPath), 0755); err != nil {
			return errors.Wrap(err, "failed to create alternates directory")
		}
		parentObjectsPath := filepath.Join(parentRepoDir, "objects")
		if err := os.WriteFile(alternatesPath, []byte(parentObjectsPath+"\n"), 0644); err != nil {
			return errors.Wrap(err, "failed to create alternates file")
		}
		fmt.Printf("Created alternates file linking to parent repository %d before fetching\n", parentRepoID)
	}

	// Add remote origin
	addRemoteCtx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()

	addRemoteCmd := exec.CommandContext(addRemoteCtx, "git", "remote", "add", "origin", remoteUrl)
	addRemoteCmd.Dir = repoDir
	if output, err := addRemoteCmd.CombinedOutput(); err != nil {
		return errors.Wrapf(err, "failed to add remote origin: %s", string(output))
	}

	// Fetch all refs with alternates optimization (should only fetch new objects)
	fetchCtx, cancel := context.WithTimeout(ctx, 30*time.Minute)
	defer cancel()

	fetchCmd := exec.CommandContext(fetchCtx, "git", "fetch", "origin", "+refs/*:refs/*")
	fetchCmd.Dir = repoDir
	if output, err := fetchCmd.CombinedOutput(); err != nil {
		return errors.Wrapf(err, "failed to fetch from origin with alternates: %s", string(output))
	}

	fmt.Printf("Successfully cloned fork with alternates optimization - only fetched unique objects\n")
	return nil
}

func main() {
	var rootCmd = &cobra.Command{
		Use:               "verify",
		Short:             "Verify IPFS packfiles contain all repository refs",
		CompletionOptions: cobra.CompletionOptions{DisableDefaultCmd: true},
		PersistentPreRunE: func(cmd *cobra.Command, _ []string) error {
			return gc.CommandInit(cmd, AppName)
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			// Get the retry-failed flag
			retryFailed, err := cmd.Flags().GetBool("retry-failed")
			if err != nil {
				return errors.Wrap(err, "failed to get retry-failed flag")
			}

			// Initialize storage manager
			storageManager := shared.NewStorageManager(viper.GetString("WORKING_DIR"))
			if err := storageManager.Load(); err != nil {
				return errors.Wrap(err, "failed to load storage manager")
			}

			// Initialize Gitopia client
			ctx := cmd.Context()
			clientCtx := client.GetClientContextFromCmd(cmd)
			txf, err := tx.NewFactoryCLI(clientCtx, cmd.Flags())
			if err != nil {
				return errors.Wrap(err, "error initializing tx factory")
			}

			gitopiaClient, err := gc.NewClient(ctx, clientCtx, txf)
			if err != nil {
				return err
			}
			defer gitopiaClient.Close()

			// Initialize IPFS cluster client
			ipfsCfg := &ipfsclusterclient.Config{
				Host:    viper.GetString("IPFS_CLUSTER_PEER_HOST"),
				Port:    viper.GetString("IPFS_CLUSTER_PEER_PORT"),
				Timeout: time.Minute * 5,
			}
			ipfsClusterClient, err := ipfsclusterclient.NewDefaultClient(ipfsCfg)
			if err != nil {
				return errors.Wrap(err, "failed to create IPFS cluster client")
			}

			// Initialize Pinata client if JWT token is provided
			var pinataClient *shared.PinataClient
			pinataJWT := viper.GetString("PINATA_JWT_TOKEN")
			if pinataJWT != "" {
				pinataClient = shared.NewPinataClient(pinataJWT)
				log.Println("Pinata client initialized")
			} else {
				log.Println("Pinata JWT token not provided, skipping Pinata uploads")
			}

			// Create temporary directory for verification
			tempDir := filepath.Join(viper.GetString("WORKING_DIR"), "verify_temp")
			if err := os.MkdirAll(tempDir, 0755); err != nil {
				return errors.Wrap(err, "failed to create temp directory")
			}
			// defer os.RemoveAll(tempDir)

			// Load verification log
			verificationLog, err := loadVerificationLog()
			if err != nil {
				return errors.Wrap(err, "failed to load verification log")
			}

			// Create parent repository manager for race condition protection
			parentRepoManager := NewParentRepoManager()

			// Handle retry-failed mode
			if retryFailed {
				fmt.Println("Starting retry of failed repositories from verification log")
				return retryFailedRepositories(ctx, &gitopiaClient, storageManager, ipfsClusterClient, pinataClient, tempDir, parentRepoManager, verificationLog)
			}

			ctx = context.Background()

			const maxRepoID uint64 = 62973
			// Get concurrency level from config, default to 10
			concurrency := viper.GetInt("VERIFY_CONCURRENCY")
			if concurrency <= 0 {
				concurrency = 20
			}

			fmt.Printf("Starting verification of repositories 0 to %d with %d concurrent workers\n", maxRepoID, concurrency)

			// Create job channel and result channel
			jobs := make(chan VerificationJob, 100)
			results := make(chan VerificationResult, 100)

			// Start worker goroutines
			var wg sync.WaitGroup
			for i := 0; i < concurrency; i++ {
				wg.Add(1)
				go func(workerID int) {
					defer wg.Done()

					for job := range jobs {
						err := verifyRepository(ctx, job.RepoID, &gitopiaClient, storageManager, tempDir, verificationLog, parentRepoManager)
						results <- VerificationResult{
							RepoID: job.RepoID,
							Error:  err,
						}
					}
				}(i)
			}

			// Start a goroutine to close results channel when all workers are done
			go func() {
				wg.Wait()
				close(results)
			}()

			// Start a goroutine to send jobs
			go func() {
				defer close(jobs)
				for repoID := uint64(0); repoID <= maxRepoID; repoID++ {
					jobs <- VerificationJob{RepoID: repoID}
				}
			}()

			// Process results and show progress
			processed := uint64(0)
			var lastSave time.Time
			for result := range results {
				processed++
				if result.Error != nil {
					fmt.Printf("Error verifying repository %d: %v\n", result.RepoID, result.Error)
				}

				// Show progress every 100 repositories
				if processed%100 == 0 {
					fmt.Printf("Verified %d/%d repositories\n", processed, maxRepoID+1)
				}

				// Save verification log every 30 seconds or every 1000 repositories
				now := time.Now()
				if processed%1000 == 0 || now.Sub(lastSave) > 30*time.Second {
					if err := saveVerificationLog(verificationLog); err != nil {
						fmt.Printf("Warning: failed to save verification log: %v\n", err)
					} else {
						lastSave = now
					}
				}
			}

			// Save final verification log
			if err := saveVerificationLog(verificationLog); err != nil {
				return errors.Wrap(err, "failed to save final verification log")
			}

			fmt.Printf("Verification complete. Found %d failures. Check %s for details.\n",
				len(verificationLog.Failures), VerificationLogFile)

			return nil
		},
	}

	// Add flags
	rootCmd.Flags().Bool("retry-failed", false, "Retry cloning repositories that failed verification")

	conf := sdk.GetConfig()
	conf.SetBech32PrefixForAccount(AccountAddressPrefix, AccountPubKeyPrefix)

	// Initialize context with logger
	ctx := logger.InitLogger(context.Background(), AppName)
	ctx = context.WithValue(ctx, client.ClientContextKey, &client.Context{})

	logger.FromContext(ctx).SetOutput(os.Stdout)

	viper.SetConfigFile("config.toml")
	viper.ReadInConfig()

	gc.WithAppName(AppName)
	gc.WithChainId(viper.GetString("CHAIN_ID"))
	gc.WithGasPrices(viper.GetString("GAS_PRICES"))
	gc.WithGitopiaAddr(viper.GetString("GITOPIA_ADDR"))
	gc.WithTmAddr(viper.GetString("TM_ADDR"))
	gc.WithWorkingDir(viper.GetString("WORKING_DIR"))

	if err := rootCmd.ExecuteContext(ctx); err != nil {
		log.Fatal(err)
	}
}
