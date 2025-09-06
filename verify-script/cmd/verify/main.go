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
	"sync"
	"time"

	"github.com/cosmos/cosmos-sdk/client"
	"github.com/cosmos/cosmos-sdk/client/tx"
	sdk "github.com/cosmos/cosmos-sdk/types"
	gc "github.com/gitopia/gitopia-go"
	"github.com/gitopia/gitopia-go/logger"
	"github.com/gitopia/gitopia-migration-tools/shared"
	gitopiatypes "github.com/gitopia/gitopia/v6/x/gitopia/types"
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
	mu       sync.Mutex             // Mutex for thread-safe access
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

// fetchPackfileFromIPFS downloads a packfile from IPFS with efficient handling for large files
func fetchPackfileFromIPFS(ctx context.Context, cid, tempDir string) (string, error) {
	// Create temporary file for the packfile
	packfilePath := filepath.Join(tempDir, fmt.Sprintf("packfile_%s.pack", cid))

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

// setupParentRepository creates parent repository from IPFS packfile for fork verification
func setupParentRepository(ctx context.Context, parentRepoID uint64, tempDir string, storageManager *shared.StorageManager) (string, error) {
	// Get parent repository packfile info from database
	parentPackfileInfo := storageManager.GetPackfileInfo(parentRepoID)
	if parentPackfileInfo == nil {
		return "", errors.Errorf("parent repository %d packfile not found in database", parentRepoID)
	}

	// Create parent repository directory
	parentRepoDir := filepath.Join(tempDir, fmt.Sprintf("parent_%d.git", parentRepoID))
	if err := os.MkdirAll(parentRepoDir, 0755); err != nil {
		return "", errors.Wrap(err, "failed to create parent repository directory")
	}

	// Initialize bare repository
	initCmd := exec.CommandContext(ctx, "git", "init", "--bare", parentRepoDir)
	if output, err := initCmd.CombinedOutput(); err != nil {
		return "", errors.Wrapf(err, "failed to initialize parent repository: %s", string(output))
	}

	// Fetch parent packfile from IPFS
	parentPackfilePath, err := fetchPackfileFromIPFS(ctx, parentPackfileInfo.CID, tempDir)
	if err != nil {
		return "", errors.Wrapf(err, "failed to fetch parent packfile from IPFS")
	}
	defer os.Remove(parentPackfilePath)

	// Import packfile into parent repository
	indexPackCmd := exec.CommandContext(ctx, "git", "index-pack", "-v", "--stdin")
	indexPackCmd.Dir = filepath.Join(parentRepoDir, "objects", "pack")
	indexPackCmd.Stdin, err = os.Open(parentPackfilePath)
	if err != nil {
		return "", errors.Wrap(err, "failed to open parent packfile")
	}

	if output, err := indexPackCmd.CombinedOutput(); err != nil {
		return "", errors.Wrapf(err, "failed to index parent packfile: %s", string(output))
	}

	return parentRepoDir, nil
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
func verifyRepository(ctx context.Context, repoID uint64, gitopiaClient *gc.Client, storageManager *shared.StorageManager, tempDir string, log *VerificationLog) error {
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

	// Get packfile info from database
	packfileInfo := storageManager.GetPackfileInfo(repoID)
	if packfileInfo == nil {
		logVerificationFailure(log, repoID, "", "", "packfile not found in database")
		return nil
	}

	// Create temporary repository directory
	repoDir := filepath.Join(tempDir, fmt.Sprintf("verify_%d.git", repoID))
	if err := os.MkdirAll(repoDir, 0755); err != nil {
		return errors.Wrap(err, "failed to create repository directory")
	}
	defer os.RemoveAll(repoDir)

	// Initialize bare repository
	initCmd := exec.CommandContext(ctx, "git", "init", "--bare", repoDir)
	if output, err := initCmd.CombinedOutput(); err != nil {
		return errors.Wrapf(err, "failed to initialize repository: %s", string(output))
	}

	// Handle fork repositories with alternates
	var parentRepoDir string
	if repo.Repository.Fork {
		parentRepoDir, err = setupParentRepository(ctx, repo.Repository.Parent, tempDir, storageManager)
		if err != nil {
			logVerificationFailure(log, repoID, "", "", fmt.Sprintf("failed to setup parent repository: %v", err))
			return nil
		}
		defer os.RemoveAll(parentRepoDir)

		// Set up alternates file
		alternatesPath := filepath.Join(repoDir, "objects", "info", "alternates")
		if err := os.MkdirAll(filepath.Dir(alternatesPath), 0755); err != nil {
			return errors.Wrap(err, "failed to create alternates directory")
		}
		parentObjectsPath := filepath.Join(parentRepoDir, "objects")
		if err := os.WriteFile(alternatesPath, []byte(parentObjectsPath+"\n"), 0644); err != nil {
			return errors.Wrap(err, "failed to create alternates file")
		}
	}

	// Fetch packfile from IPFS
	packfilePath, err := fetchPackfileFromIPFS(ctx, packfileInfo.CID, tempDir)
	if err != nil {
		logVerificationFailure(log, repoID, "", "", fmt.Sprintf("failed to fetch packfile from IPFS: %v", err))
		return nil
	}
	defer os.Remove(packfilePath)

	// Import packfile into repository
	packDir := filepath.Join(repoDir, "objects", "pack")
	if err := os.MkdirAll(packDir, 0755); err != nil {
		return errors.Wrap(err, "failed to create pack directory")
	}

	indexPackCmd := exec.CommandContext(ctx, "git", "index-pack", "-v", "--stdin")
	indexPackCmd.Dir = packDir
	indexPackCmd.Stdin, err = os.Open(packfilePath)
	if err != nil {
		logVerificationFailure(log, repoID, "", "", fmt.Sprintf("failed to open packfile: %v", err))
		return nil
	}

	if output, err := indexPackCmd.CombinedOutput(); err != nil {
		logVerificationFailure(log, repoID, "", "", fmt.Sprintf("failed to index packfile: %v, output: %s", err, string(output)))
		return nil
	}

	// Query all branches and tags from Gitopia
	branchAllRes, err := gitopiaClient.QueryClient().Gitopia.RepositoryBranchAll(ctx, &gitopiatypes.QueryAllRepositoryBranchRequest{
		Id:             repo.Repository.Owner.Id,
		RepositoryName: repo.Repository.Name,
	})
	if err != nil {
		logVerificationFailure(log, repoID, "", "", fmt.Sprintf("failed to query branches: %v", err))
		return nil
	}

	tagAllRes, err := gitopiaClient.QueryClient().Gitopia.RepositoryTagAll(ctx, &gitopiatypes.QueryAllRepositoryTagRequest{
		Id:             repo.Repository.Owner.Id,
		RepositoryName: repo.Repository.Name,
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

func main() {
	var rootCmd = &cobra.Command{
		Use:               "verify",
		Short:             "Verify IPFS packfiles contain all repository refs",
		CompletionOptions: cobra.CompletionOptions{DisableDefaultCmd: true},
		PersistentPreRunE: func(cmd *cobra.Command, _ []string) error {
			return gc.CommandInit(cmd, AppName)
		},
		RunE: func(cmd *cobra.Command, args []string) error {

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

			// Create temporary directory for verification
			tempDir := filepath.Join(viper.GetString("WORKING_DIR"), "verify_temp")
			if err := os.MkdirAll(tempDir, 0755); err != nil {
				return errors.Wrap(err, "failed to create temp directory")
			}
			defer os.RemoveAll(tempDir)

			// Load verification log
			verificationLog, err := loadVerificationLog()
			if err != nil {
				return errors.Wrap(err, "failed to load verification log")
			}

			ctx = context.Background()

			const maxRepoID uint64 = 62973
			// Get concurrency level from config, default to 10
			concurrency := viper.GetInt("VERIFY_CONCURRENCY")
			if concurrency <= 0 {
				concurrency = 10
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
					// Create worker-specific temp directory
					workerTempDir := filepath.Join(tempDir, fmt.Sprintf("worker_%d", workerID))
					if err := os.MkdirAll(workerTempDir, 0755); err != nil {
						fmt.Printf("Worker %d: failed to create temp directory: %v\n", workerID, err)
						return
					}
					defer os.RemoveAll(workerTempDir)

					for job := range jobs {
						err := verifyRepository(ctx, job.RepoID, &gitopiaClient, storageManager, workerTempDir, verificationLog)
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

	if err := rootCmd.Execute(); err != nil {
		log.Fatal(err)
	}
}
