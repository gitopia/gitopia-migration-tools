package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"github.com/cosmos/cosmos-sdk/client"
	"github.com/cosmos/cosmos-sdk/client/keys"
	"github.com/cosmos/cosmos-sdk/client/tx"
	sdk "github.com/cosmos/cosmos-sdk/types"
	gc "github.com/gitopia/gitopia-go"
	"github.com/gitopia/gitopia-go/logger"
	"github.com/gitopia/gitopia-migration-tools/shared"
	gitopiatypes "github.com/gitopia/gitopia/v5/x/gitopia/types"
	ipfsclusterclient "github.com/ipfs-cluster/ipfs-cluster/api/rest/client"
	"github.com/pkg/errors"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

const (
	AccountAddressPrefix = "gitopia"
	AccountPubKeyPrefix  = AccountAddressPrefix + sdk.PrefixPublic
	AppName              = "gitopia-clone-script"
	ProgressFile         = "gitopia_clone_progress.json"
	ReleaseProgressFile  = "gitopia_clone_releases_progress.json"
)

type CloneProgress struct {
	RepositoryNextKey      []byte            `json:"repository_next_key"`
	ReleaseNextKey         []byte            `json:"release_next_key"`
	FailedRepos            map[uint64]string `json:"failed_repos"`
	FailedReleases         map[uint64]string `json:"failed_releases"`
	LastFailedRepo         uint64            `json:"last_failed_repo"`
	LastFailedRelease      uint64            `json:"last_failed_release"`
	LastProcessedRepoID    uint64            `json:"last_processed_repo_id"`
	LastProcessedReleaseID uint64            `json:"last_processed_release_id"`
	ProcessedReleases      map[uint64]bool   `json:"processed_releases"`
	CurrentBatchRepos      []uint64          `json:"current_batch_repos"`
	CurrentBatchReleases   []uint64          `json:"current_batch_releases"`
}

func loadProgress(releasesOnly bool) (*CloneProgress, error) {
	progressFile := ProgressFile
	if releasesOnly {
		progressFile = ReleaseProgressFile
	}
	
	if _, err := os.Stat(progressFile); os.IsNotExist(err) {
		return &CloneProgress{
			FailedRepos:       make(map[uint64]string),
			FailedReleases:    make(map[uint64]string),
			ProcessedReleases: make(map[uint64]bool),
		}, nil
	}

	data, err := os.ReadFile(progressFile)
	if err != nil {
		return nil, err
	}

	var progress CloneProgress
	if err := json.Unmarshal(data, &progress); err != nil {
		return nil, err
	}

	if progress.FailedRepos == nil {
		progress.FailedRepos = make(map[uint64]string)
	}
	if progress.FailedReleases == nil {
		progress.FailedReleases = make(map[uint64]string)
	}
	if progress.ProcessedReleases == nil {
		progress.ProcessedReleases = make(map[uint64]bool)
	}

	return &progress, nil
}

func saveProgress(progress *CloneProgress, releasesOnly bool) error {
	progressFile := ProgressFile
	if releasesOnly {
		progressFile = ReleaseProgressFile
	}
	
	data, err := json.Marshal(progress)
	if err != nil {
		return errors.Wrap(err, "failed to marshal progress")
	}
	return os.WriteFile(progressFile, data, 0644)
}

// Helper function to check if a repository should be processed
func shouldProcessRepo(progress *CloneProgress, repoID uint64) bool {
	// Process if it's a failed repo (retry)
	if _, isFailed := progress.FailedRepos[repoID]; isFailed {
		return true
	}

	// Process if it's a new repo (ID > last processed)
	return repoID > progress.LastProcessedRepoID
}

// Helper function to check if a release should be processed
func shouldProcessRelease(progress *CloneProgress, releaseID uint64) bool {
	// Skip if already processed successfully
	if progress.ProcessedReleases[releaseID] {
		return false
	}

	// Process if it's a failed release (retry)
	if _, isFailed := progress.FailedReleases[releaseID]; isFailed {
		return true
	}

	// Process if it's a new release (ID > last processed)
	return releaseID > progress.LastProcessedReleaseID
}

// Atomic operation for repository processing
func processRepositoryAtomic(ctx context.Context, repository *gitopiatypes.Repository, gitDir string,
	ipfsClusterClient ipfsclusterclient.Client, storageManager *shared.StorageManager,
	gitopiaClient *gc.Client, progress *CloneProgress, pinataClient *shared.PinataClient) error {

	repoID := repository.Id

	defer func() {
		// On any error, ensure we don't mark as processed
		if r := recover(); r != nil {
			progress.FailedRepos[repoID] = fmt.Sprintf("panic: %v", r)
			progress.LastFailedRepo = repoID
			panic(r)
		}
	}()

	// Check if repository packfile already exists in database (might be processed by sync daemon)
	existingPackfile := storageManager.GetPackfileInfo(repoID)
	if existingPackfile != nil && existingPackfile.UpdatedBy == "sync" {
		fmt.Printf("Repository %d already processed by sync daemon, skipping\n", repoID)
		return nil
	}

	// Check if repository is empty
	_, err := gitopiaClient.QueryClient().Gitopia.RepositoryBranch(ctx, &gitopiatypes.QueryGetRepositoryBranchRequest{
		Id:             repository.Owner.Id,
		RepositoryName: repository.Name,
		BranchName:     repository.DefaultBranch,
	})
	if err != nil {
		fmt.Printf("Repository %d is empty, skipping\n", repoID)
		return nil
	}

	// Clone repository
	repoDir := filepath.Join(gitDir, fmt.Sprintf("%d.git", repoID))
	remoteUrl := fmt.Sprintf("gitopia://%s/%s", repository.Owner.Id, repository.Name)
	
	// Create context with timeout for git clone (30 minutes)
	cloneCtx, cancel := context.WithTimeout(ctx, 30*time.Minute)
	defer cancel()
	
	cmd := exec.CommandContext(cloneCtx, "git", "clone", "--bare", "--mirror", remoteUrl, repoDir)
	if err := cmd.Run(); err != nil {
		return errors.Wrapf(err, "error cloning repository %d", repoID)
	}

	// Handle forked repository optimization
	if repository.Fork {
		fmt.Printf("Repository %d is a fork of %d, setting up alternates\n", repoID, repository.Parent)

		// Ensure parent repository is processed first
		parentRepoDir := filepath.Join(gitDir, fmt.Sprintf("%d.git", repository.Parent))
		if _, err := os.Stat(parentRepoDir); os.IsNotExist(err) {
			// Load parent repository if it doesn't exist
			if err := loadParentRepository(ctx, repository.Parent, gitDir, ipfsClusterClient, storageManager, gitopiaClient); err != nil {
				return errors.Wrapf(err, "error loading parent repository %d for fork %d", repository.Parent, repoID)
			}
		}

		if _, err := os.Stat(parentRepoDir); err == nil {
			alternatesPath := filepath.Join(repoDir, "objects", "info", "alternates")
			if err := os.MkdirAll(filepath.Dir(alternatesPath), 0755); err != nil {
				return errors.Wrapf(err, "error creating alternates directory for repo %d", repoID)
			}
			parentObjectsPath := filepath.Join(parentRepoDir, "objects")
			if err := os.WriteFile(alternatesPath, []byte(parentObjectsPath+"\n"), 0644); err != nil {
				return errors.Wrapf(err, "error creating alternates file for repo %d", repoID)
			}
			fmt.Printf("Created alternates file linking to parent repository %d\n", repository.Parent)
		}
	}

	// Run git gc with timeout (10 minutes)
	gcCtx, cancel := context.WithTimeout(ctx, 10*time.Minute)
	defer cancel()
	
	cmd = exec.CommandContext(gcCtx, "git", "gc")
	cmd.Dir = repoDir
	if err := cmd.Run(); err != nil {
		return errors.Wrapf(err, "error running git gc for repo %d", repoID)
	}

	// For forked repos with alternates, run git repack to remove common objects
	if repository.Fork {
		alternatesPath := filepath.Join(repoDir, "objects", "info", "alternates")
		if _, err := os.Stat(alternatesPath); err == nil {
			fmt.Printf("Running git repack to optimize forked repository %d\n", repoID)
			
			// Create context with timeout for git repack (15 minutes)
			repackCtx, cancel := context.WithTimeout(ctx, 15*time.Minute)
			defer cancel()
			
			cmd = exec.CommandContext(repackCtx, "git", "repack", "-a", "-d", "-l")
			cmd.Dir = repoDir
			if err := cmd.Run(); err != nil {
				fmt.Printf("Warning: git repack failed for forked repo %d: %v\n", repoID, err)
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
			UpdatedBy:    "clone",
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

		// Delete the repository directory after successful pinning and storage
		if err := os.RemoveAll(repoDir); err != nil {
			fmt.Printf("Warning: failed to delete repository directory %s: %v\n", repoDir, err)
		} else {
			fmt.Printf("Successfully deleted repository directory for repo %d\n", repoID)
		}
	} else if repository.Fork {
		fmt.Printf("Forked repository %d has no packfile (no new changes), using parent objects via alternates\n", repoID)

		// Delete the repository directory after processing fork
		if err := os.RemoveAll(repoDir); err != nil {
			fmt.Printf("Warning: failed to delete forked repository directory %s: %v\n", repoDir, err)
		} else {
			fmt.Printf("Successfully deleted forked repository directory for repo %d\n", repoID)
		}
	}

	// Process LFS objects
	if err := processLFSObjects(ctx, repoID, repoDir, ipfsClusterClient, storageManager, pinataClient); err != nil {
		return errors.Wrapf(err, "error processing LFS objects for repo %d", repoID)
	}

	return nil
}

// loadParentRepository loads a parent repository for forked repos
func loadParentRepository(ctx context.Context, parentRepoID uint64, gitDir string, ipfsClusterClient ipfsclusterclient.Client, storageManager *shared.StorageManager, gitopiaClient *gc.Client) error {
	// Check if parent packfile exists in storage manager
	parentPackfile := storageManager.GetPackfileInfo(parentRepoID)
	if parentPackfile == nil {
		return errors.Errorf("parent repository %d packfile not found in storage", parentRepoID)
	}

	// Get parent repository info
	parentRepository, err := gitopiaClient.QueryClient().Gitopia.Repository(ctx, &gitopiatypes.QueryGetRepositoryRequest{
		Id: parentRepoID,
	})
	if err != nil {
		return errors.Wrap(err, "failed to get parent repository")
	}

	// Check if parent repository is empty
	branch, err := gitopiaClient.QueryClient().Gitopia.RepositoryBranch(ctx, &gitopiatypes.QueryGetRepositoryBranchRequest{
		Id:             parentRepository.Repository.Owner.Id,
		RepositoryName: parentRepository.Repository.Name,
		BranchName:     parentRepository.Repository.DefaultBranch,
	})
	if err != nil {
		return errors.Wrapf(err, "error getting parent repository branches for repo %d", parentRepoID)
	}
	if branch.Branch.Name == "" {
		fmt.Printf("Parent repository %d is empty, skipping\n", parentRepoID)
		return nil
	}

	// Initialize parent repository directory
	parentRepoDir := filepath.Join(gitDir, fmt.Sprintf("%d.git", parentRepoID))
	if _, err := os.Stat(filepath.Join(parentRepoDir, "objects")); os.IsNotExist(err) {
			// Create context with timeout for git init (5 minutes)
		initCtx, cancel := context.WithTimeout(ctx, 5*time.Minute)
		defer cancel()
		
		cmd := exec.CommandContext(initCtx, "git", "init", "--bare", parentRepoDir)
		if err := cmd.Run(); err != nil {
			return errors.Wrapf(err, "failed to initialize parent repository %d", parentRepoID)
		}
	}

	// Download packfile from IPFS cluster using CID from storage
	if err := downloadPackfileFromIPFS(parentPackfile.CID, parentPackfile.Name, parentRepoDir); err != nil {
		return errors.Wrapf(err, "error downloading parent packfile for repo %d", parentRepoID)
	}

	// Recursively load parent's parent if it's also a fork
	if parentRepository.Repository.Fork {
		grandParentRepoDir := filepath.Join(gitDir, fmt.Sprintf("%d.git", parentRepository.Repository.Parent))
		if _, err := os.Stat(grandParentRepoDir); os.IsNotExist(err) {
			if err := loadParentRepository(ctx, parentRepository.Repository.Parent, gitDir, ipfsClusterClient, storageManager, gitopiaClient); err != nil {
				return errors.Wrapf(err, "error loading grandparent repository %d", parentRepository.Repository.Parent)
			}
		}

		// Set up alternates for parent repository
		if _, err := os.Stat(grandParentRepoDir); err == nil {
			alternatesPath := filepath.Join(parentRepoDir, "objects", "info", "alternates")
			if err := os.MkdirAll(filepath.Dir(alternatesPath), 0755); err != nil {
				return errors.Wrapf(err, "error creating alternates directory for parent repo %d", parentRepoID)
			}
			grandParentObjectsPath := filepath.Join(grandParentRepoDir, "objects")
			if err := os.WriteFile(alternatesPath, []byte(grandParentObjectsPath+"\n"), 0644); err != nil {
				return errors.Wrapf(err, "error creating alternates file for parent repo %d", parentRepoID)
			}
			fmt.Printf("Created alternates file linking parent repository %d to grandparent %d\n", parentRepoID, parentRepository.Repository.Parent)
		}
	}

	// Run git gc on parent repository with timeout (10 minutes)
	parentGcCtx, cancel := context.WithTimeout(ctx, 10*time.Minute)
	defer cancel()
	
	cmd := exec.CommandContext(parentGcCtx, "git", "gc")
	cmd.Dir = parentRepoDir
	if err := cmd.Run(); err != nil {
		return errors.Wrapf(err, "error running git gc for parent repo %d", parentRepoID)
	}

	// Parent packfile is already stored in IPFS cluster, no need to pin again
	fmt.Printf("Successfully loaded parent repository %d from IPFS (CID: %s)\n", parentRepoID, parentPackfile.CID)

	return nil
}

// downloadPackfileFromIPFS downloads a packfile from IPFS cluster and sets up the repository
func downloadPackfileFromIPFS(cid, packfileName, repoDir string) error {
	// Download packfile from IPFS cluster
	packDir := filepath.Join(repoDir, "objects", "pack")
	if err := os.MkdirAll(packDir, 0755); err != nil {
		return errors.Wrap(err, "failed to create pack directory")
	}

	packfilePath := filepath.Join(packDir, packfileName)
	if err := downloadFromIPFSCluster(cid, packfilePath); err != nil {
		return errors.Wrap(err, "failed to download packfile from IPFS")
	}

	// Build pack index file with timeout (10 minutes)
	indexCtx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	
	cmd := exec.CommandContext(indexCtx, "git", "index-pack", packfilePath)
	cmd.Dir = repoDir
	if err := cmd.Run(); err != nil {
		return errors.Wrap(err, "failed to build pack index")
	}

	return nil
}

// downloadFromIPFSCluster downloads a file from IPFS cluster using HTTP API
func downloadFromIPFSCluster(cid, filePath string) error {
	ipfsUrl := fmt.Sprintf("http://%s:%s/api/v0/cat?arg=/ipfs/%s&progress=false",
		viper.GetString("IPFS_HOST"),
		viper.GetString("IPFS_PORT"),
		cid)

	resp, err := http.Post(ipfsUrl, "application/json", nil)
	if err != nil {
		return fmt.Errorf("failed to fetch file from IPFS: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("failed to fetch file from IPFS: %v", resp.Status)
	}

	file, err := os.Create(filePath)
	if err != nil {
		return fmt.Errorf("failed to create file: %v", err)
	}
	defer file.Close()

	if _, err := io.Copy(file, resp.Body); err != nil {
		return fmt.Errorf("failed to write file: %v", err)
	}

	return nil
}

func processLFSObjects(ctx context.Context, repositoryId uint64, repoDir string, ipfsClusterClient ipfsclusterclient.Client, storageManager *shared.StorageManager, pinataClient *shared.PinataClient) error {
	lfsObjectsDir := filepath.Join(repoDir, "lfs", "objects")

	if _, err := os.Stat(lfsObjectsDir); os.IsNotExist(err) {
		fmt.Printf("No LFS objects directory found for repository %d\n", repositoryId)
		return nil
	}

	var lfsObjects []string
	err := filepath.Walk(lfsObjectsDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		if info.IsDir() || !info.Mode().IsRegular() {
			return nil
		}

		relPath, err := filepath.Rel(lfsObjectsDir, path)
		if err != nil {
			return err
		}

		pathParts := strings.Split(relPath, string(filepath.Separator))
		if len(pathParts) >= 3 {
			oid := pathParts[len(pathParts)-1]
			if len(oid) == 64 {
				lfsObjects = append(lfsObjects, oid)
			}
		}

		return nil
	})

	if err != nil {
		return errors.Wrap(err, "failed to walk LFS objects directory")
	}

	if len(lfsObjects) == 0 {
		fmt.Printf("No LFS objects found for repository %d\n", repositoryId)
		return nil
	}

	fmt.Printf("Found %d LFS objects for repository %d\n", len(lfsObjects), repositoryId)

	for i, oid := range lfsObjects {
		fmt.Printf("Processing LFS object %d/%d: %s\n", i+1, len(lfsObjects), oid)

		oidPath := filepath.Join(lfsObjectsDir, oid[:2], oid[2:])

		cid, err := shared.PinFile(ipfsClusterClient, oidPath)
		if err != nil {
			return errors.Wrapf(err, "error pinning LFS object %s", oid)
		}

		// Compute merkle root and file info
		rootHash, size, err := shared.ComputeFileInfo(oidPath, cid)
		if err != nil {
			return errors.Wrapf(err, "error computing file info for LFS object %s", oid)
		}

		// Store LFS object information
		lfsInfo := &shared.LFSObjectInfo{
			RepositoryID: repositoryId,
			OID:          oid,
			CID:          cid,
			RootHash:     rootHash,
			Size:         size,
			UpdatedAt:    time.Now(),
			UpdatedBy:    "clone",
		}
		storageManager.SetLFSObjectInfo(lfsInfo)

		fmt.Printf("Successfully pinned LFS object %s (CID: %s)\n", oid, cid)

		// Pin to Pinata if enabled
		if pinataClient != nil {
			resp, err := pinataClient.PinFile(ctx, oidPath, oid)
			if err != nil {
				fmt.Printf("Warning: failed to pin LFS object to Pinata for repo %d, oid %s: %v\n", repositoryId, oid, err)
			} else {
				fmt.Printf("Successfully pinned LFS object to Pinata for repository %d, oid %s (Pinata ID: %s)\n", repositoryId, oid, resp.Data.ID)
			}
		}

		// Delete the LFS object file after successful pinning
		if err := os.Remove(oidPath); err != nil {
			fmt.Printf("Warning: failed to delete LFS object file %s: %v\n", oidPath, err)
		} else {
			fmt.Printf("Successfully deleted LFS object file %s\n", oidPath)
		}
	}

	return nil
}

func main() {
	rootCmd := &cobra.Command{
		Use:               "clone-script",
		Short:             "Clone existing repositories and releases to IPFS",
		CompletionOptions: cobra.CompletionOptions{DisableDefaultCmd: true},
		PersistentPreRunE: func(cmd *cobra.Command, _ []string) error {
			return gc.CommandInit(cmd, AppName)
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			// Get the releases-only flag
			releasesOnly, err := cmd.Flags().GetBool("releases-only")
			if err != nil {
				return errors.Wrap(err, "failed to get releases-only flag")
			}

			// Create required directories
			gitDir := viper.GetString("GIT_REPOS_DIR")
			if err := os.MkdirAll(gitDir, 0755); err != nil {
				return errors.Wrap(err, "failed to create git repositories directory")
			}

			attachmentDir := viper.GetString("ATTACHMENT_DIR")
			if err := os.MkdirAll(attachmentDir, 0755); err != nil {
				return errors.Wrap(err, "failed to create attachments directory")
			}

			// Load progress
			progress, err := loadProgress(releasesOnly)
			if err != nil {
				return errors.Wrap(err, "failed to load progress")
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

			// Process repositories only if not in releases-only mode
			if !releasesOnly {
				// Process repositories using direct ID queries (0 to 62973)
				var processedCount int
				const maxRepositoryID uint64 = 62973
				fmt.Printf("Starting repository processing from ID 0 to %d\n", maxRepositoryID)

				// Determine starting ID based on progress
				startID := uint64(0)
				if progress.LastProcessedRepoID > 0 {
					startID = progress.LastProcessedRepoID + 1
				}

				for repoID := startID; repoID <= maxRepositoryID; repoID++ {
					// Use improved resume logic
					if !shouldProcessRepo(progress, repoID) {
						processedCount++
						continue
					}

					fmt.Printf("Querying repository ID %d\n", repoID)
					repositoryResp, err := gitopiaClient.QueryClient().Gitopia.Repository(ctx, &gitopiatypes.QueryGetRepositoryRequest{
						Id: repoID,
					})
					if err != nil {
						// Repository might not exist, skip and continue
						fmt.Printf("Repository ID %d not found, skipping: %v\n", repoID, err)
						processedCount++
						continue
					}

					repository := repositoryResp.Repository

					fmt.Printf("Processing repository %d (%d/%d)\n", repository.Id, processedCount+1, maxRepositoryID+1)

					// Use atomic processing function
					if err := processRepositoryAtomic(ctx, repository, gitDir, ipfsClusterClient, storageManager, &gitopiaClient, progress, pinataClient); err != nil {
						progress.FailedRepos[repository.Id] = err.Error()
						progress.LastFailedRepo = repository.Id
						if err := saveProgress(progress, releasesOnly); err != nil {
							return errors.Wrap(err, "failed to save progress")
						}
						return errors.Wrapf(err, "error processing repository %d", repository.Id)
					}

					// Mark repository as successfully processed
					delete(progress.FailedRepos, repository.Id)
					progress.LastProcessedRepoID = repository.Id
					if err := saveProgress(progress, releasesOnly); err != nil {
						return errors.Wrap(err, "failed to save progress")
					}

					// Save storage manager state
					if err := storageManager.Save(); err != nil {
						return errors.Wrap(err, "failed to save storage manager")
					}

					processedCount++
					fmt.Printf("Successfully cloned repository %d\n", repository.Id)
				}

				fmt.Printf("Repository processing complete - processed all IDs from 0 to %d\n", maxRepositoryID)
			} else {
				fmt.Println("Skipping repository processing (releases-only mode)")
			}

			// Process releases (all fetched in single request)
			var processedCount int
			fmt.Printf("Starting release processing\n")

			releases, err := gitopiaClient.QueryClient().Gitopia.ReleaseAll(ctx, &gitopiatypes.QueryAllReleaseRequest{})
			if err != nil {
				return errors.Wrap(err, "failed to get releases")
			}

			totalReleases := uint64(len(releases.Release))
			fmt.Printf("Total releases to process: %d\n", totalReleases)

			for _, release := range releases.Release {
				// Use improved resume logic
				if !shouldProcessRelease(progress, release.Id) {
					processedCount++
					continue
				}

				fmt.Printf("Processing release %s for repository %d (%d/%d)\n", release.TagName, release.RepositoryId, processedCount+1, totalReleases)

				if len(release.Attachments) == 0 {
					// Mark release as processed even if no attachments
					progress.ProcessedReleases[release.Id] = true
					progress.LastProcessedReleaseID = release.Id
					processedCount++
					continue
				}

				// Fetch repository
				repository, err := gitopiaClient.QueryClient().Gitopia.Repository(ctx, &gitopiatypes.QueryGetRepositoryRequest{
					Id: release.RepositoryId,
				})
				if err != nil {
					progress.FailedReleases[release.Id] = err.Error()
					progress.LastFailedRelease = release.Id
					if err := saveProgress(progress, releasesOnly); err != nil {
						return errors.Wrap(err, "failed to save progress")
					}
					return errors.Wrap(err, "error getting repository")
				}

				for _, attachment := range release.Attachments {
					// Check if release asset already exists in database (might be processed by sync daemon)
					existingAsset := storageManager.GetReleaseAssetInfo(release.RepositoryId, release.TagName, attachment.Name)
					if existingAsset != nil && existingAsset.UpdatedBy == "sync" {
						fmt.Printf("Release asset %s for repository %d tag %s already processed by sync daemon, skipping\n", attachment.Name, release.RepositoryId, release.TagName)
						continue
					}

					// Download release asset
					attachmentUrl := fmt.Sprintf("%s/releases/%s/%s/%s/%s",
						viper.GetString("GIT_SERVER_HOST"),
						repository.Repository.Owner.Id,
						repository.Repository.Name,
						release.TagName,
						attachment.Name)

					filePath := filepath.Join(attachmentDir, attachment.Sha)
					
					// Create context with timeout for wget (20 minutes)
					wgetCtx, cancel := context.WithTimeout(ctx, 20*time.Minute)
					defer cancel()
					
					cmd := exec.CommandContext(wgetCtx, "wget", attachmentUrl, "-O", filePath)
					output, err := cmd.CombinedOutput()
					if err != nil {
						progress.FailedReleases[release.Id] = err.Error()
						progress.LastFailedRelease = release.Id
						if err := saveProgress(progress, releasesOnly); err != nil {
							return errors.Wrap(err, "failed to save progress")
						}
						return errors.Wrapf(err, "error downloading release asset: %s", string(output))
					}

					// Verify sha256 with timeout (5 minutes)
					shaCtx, cancel := context.WithTimeout(ctx, 5*time.Minute)
					defer cancel()
					
					cmd = exec.CommandContext(shaCtx, "sha256sum", filePath)
					output, err = cmd.CombinedOutput()
					if err != nil {
						progress.FailedReleases[release.Id] = err.Error()
						progress.LastFailedRelease = release.Id
						if err := saveProgress(progress, releasesOnly); err != nil {
							return errors.Wrap(err, "failed to save progress")
						}
						return errors.Wrap(err, "error verifying sha256 for attachment")
					}
					calculatedHash := strings.Fields(string(output))[0]
					if calculatedHash != attachment.Sha {
						err := errors.Errorf("SHA256 mismatch for attachment %s: %s != %s", attachment.Name, calculatedHash, attachment.Sha)
						progress.FailedReleases[release.Id] = err.Error()
						progress.LastFailedRelease = release.Id
						if err := saveProgress(progress, releasesOnly); err != nil {
							return errors.Wrap(err, "failed to save progress")
						}
						return err
					}

					// Pin to IPFS cluster
					cid, err := shared.PinFile(ipfsClusterClient, filePath)
					if err != nil {
						progress.FailedReleases[release.Id] = err.Error()
						progress.LastFailedRelease = release.Id
						if err := saveProgress(progress, releasesOnly); err != nil {
							return errors.Wrap(err, "failed to save progress")
						}
						return errors.Wrap(err, "error pinning attachment")
					}

					// Pin to Pinata if enabled
					if pinataClient != nil {
						name := fmt.Sprintf("release-%d-%s-%s-%s", release.RepositoryId, release.TagName, attachment.Name, attachment.Sha)
						resp, err := pinataClient.PinFile(ctx, filePath, name)
						if err != nil {
							fmt.Printf("Warning: failed to pin release asset to Pinata for repo %d, tag %s, asset %s: %v\n", release.RepositoryId, release.TagName, attachment.Name, err)
						} else {
							fmt.Printf("Successfully pinned release asset to Pinata for repository %d, tag %s, asset %s (Pinata ID: %s)\n", release.RepositoryId, release.TagName, attachment.Name, resp.Data.ID)
						}
					}

					// Compute merkle root and file info
					rootHash, size, err := shared.ComputeFileInfo(filePath, cid)
					if err != nil {
						progress.FailedReleases[release.Id] = err.Error()
						progress.LastFailedRelease = release.Id
						if err := saveProgress(progress, releasesOnly); err != nil {
							return errors.Wrap(err, "failed to save progress")
						}
						return errors.Wrapf(err, "error computing file info for attachment %s", attachment.Name)
					}

					// Store release asset information
					assetInfo := &shared.ReleaseAssetInfo{
						RepositoryID: release.RepositoryId,
						TagName:      release.TagName,
						Name:         attachment.Name,
						CID:          cid,
						RootHash:     rootHash,
						Size:         size,
						SHA256:       attachment.Sha,
						UpdatedAt:    time.Now(),
						UpdatedBy:    "clone",
					}
					storageManager.SetReleaseAssetInfo(assetInfo)

					fmt.Printf("Successfully downloaded and pinned attachment %s for release %s (CID: %s)\n", attachment.Name, release.TagName, cid)

					// Delete the attachment file after successful pinning
					if err := os.Remove(filePath); err != nil {
						fmt.Printf("Warning: failed to delete attachment file %s: %v\n", filePath, err)
					} else {
						fmt.Printf("Successfully deleted attachment file %s\n", filePath)
					}
				}

				// Mark release as successfully processed
				delete(progress.FailedReleases, release.Id)
				progress.ProcessedReleases[release.Id] = true
				progress.LastProcessedReleaseID = release.Id
				if err := saveProgress(progress, releasesOnly); err != nil {
					return errors.Wrap(err, "failed to save progress")
				}

				// Save storage manager state
				if err := storageManager.Save(); err != nil {
					return errors.Wrap(err, "failed to save storage manager")
				}

				processedCount++
			}

			fmt.Printf("Release processing complete - processed %d releases\n", processedCount)

			fmt.Println("Clone script completed successfully!")
			return nil
		},
	}

	// Add flags
	rootCmd.Flags().String("from", "", "Name or address of private key with which to sign")
	rootCmd.Flags().String("keyring-backend", "", "Select keyring's backend (os|file|kwallet|pass|test|memory)")
	rootCmd.Flags().Bool("releases-only", false, "Process only releases, skip repository cloning")

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

	rootCmd.AddCommand(keys.Commands(viper.GetString("WORKING_DIR")))

	if err := rootCmd.ExecuteContext(ctx); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
