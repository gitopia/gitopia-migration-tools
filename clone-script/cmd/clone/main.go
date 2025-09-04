package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"github.com/cosmos/cosmos-sdk/client"
	"github.com/cosmos/cosmos-sdk/client/keys"
	"github.com/cosmos/cosmos-sdk/client/tx"
	sdk "github.com/cosmos/cosmos-sdk/types"
	"github.com/cosmos/cosmos-sdk/types/query"
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
	AppName              = "clone-script"
	ProgressFile         = "clone_progress.json"
)

type CloneProgress struct {
	RepositoryNextKey     []byte            `json:"repository_next_key"`
	ReleaseNextKey        []byte            `json:"release_next_key"`
	FailedRepos           map[uint64]string `json:"failed_repos"`
	FailedReleases        map[uint64]string `json:"failed_releases"`
	LastFailedRepo        uint64            `json:"last_failed_repo"`
	LastFailedRelease     uint64            `json:"last_failed_release"`
	LastProcessedRepoID   uint64            `json:"last_processed_repo_id"`
	LastProcessedReleaseID uint64           `json:"last_processed_release_id"`
	ProcessedRepos        map[uint64]bool   `json:"processed_repos"`
	ProcessedReleases     map[uint64]bool   `json:"processed_releases"`
	CurrentBatchRepos     []uint64          `json:"current_batch_repos"`
	CurrentBatchReleases  []uint64          `json:"current_batch_releases"`
}

func loadProgress() (*CloneProgress, error) {
	if _, err := os.Stat(ProgressFile); os.IsNotExist(err) {
		return &CloneProgress{
			FailedRepos:       make(map[uint64]string),
			FailedReleases:    make(map[uint64]string),
			ProcessedRepos:    make(map[uint64]bool),
			ProcessedReleases: make(map[uint64]bool),
		}, nil
	}

	data, err := os.ReadFile(ProgressFile)
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
	if progress.ProcessedRepos == nil {
		progress.ProcessedRepos = make(map[uint64]bool)
	}
	if progress.ProcessedReleases == nil {
		progress.ProcessedReleases = make(map[uint64]bool)
	}

	return &progress, nil
}

func saveProgress(progress *CloneProgress) error {
	data, err := json.Marshal(progress)
	if err != nil {
		return errors.Wrap(err, "failed to marshal progress")
	}
	return os.WriteFile(ProgressFile, data, 0644)
}

// Helper function to check if a repository should be processed
func shouldProcessRepo(progress *CloneProgress, repoID uint64) bool {
	// Skip if already processed successfully
	if progress.ProcessedRepos[repoID] {
		return false
	}
	
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

// Helper function to validate fork repository dependencies
func validateForkDependency(progress *CloneProgress, repository *gitopiatypes.Repository, gitDir string) error {
	if !repository.Fork {
		return nil
	}
	
	parentRepoDir := filepath.Join(gitDir, fmt.Sprintf("%d.git", repository.Parent))
	if _, err := os.Stat(parentRepoDir); os.IsNotExist(err) {
		// Check if parent is in current batch or already processed
		if !progress.ProcessedRepos[repository.Parent] {
			return errors.Errorf("parent repository %d not found and not processed yet for fork %d", repository.Parent, repository.Id)
		}
	}
	return nil
}

// Atomic operation for repository processing
func processRepositoryAtomic(ctx context.Context, repository *gitopiatypes.Repository, gitDir string, 
	ipfsClusterClient ipfsclusterclient.Client, storageManager *shared.StorageManager, 
	gitopiaClient *gc.Client, progress *CloneProgress) error {
	
	repoID := repository.Id
	
	defer func() {
		// On any error, ensure we don't mark as processed
		if r := recover(); r != nil {
			progress.FailedRepos[repoID] = fmt.Sprintf("panic: %v", r)
			progress.LastFailedRepo = repoID
			panic(r)
		}
	}()
	
	// Check if repository is empty
	branch, err := gitopiaClient.QueryClient().Gitopia.RepositoryBranch(ctx, &gitopiatypes.QueryGetRepositoryBranchRequest{
		Id:             repository.Owner.Id,
		RepositoryName: repository.Name,
		BranchName:     repository.DefaultBranch,
	})
	if err != nil {
		return errors.Wrapf(err, "error getting repository branches for repo %d", repoID)
	}
	if branch.Branch.Name == "" {
		fmt.Printf("Repository %d is empty, skipping\n", repoID)
		return nil
	}

	// Clone repository
	repoDir := filepath.Join(gitDir, fmt.Sprintf("%d.git", repoID))
	remoteUrl := fmt.Sprintf("gitopia://%s/%s", repository.Owner.Id, repository.Name)
	cmd := exec.Command("git", "clone", "--bare", remoteUrl, repoDir)
	if err := cmd.Run(); err != nil {
		return errors.Wrapf(err, "error cloning repository %d", repoID)
	}

	// Handle forked repository optimization
	if repository.Fork {
		fmt.Printf("Repository %d is a fork of %d, setting up alternates\n", repoID, repository.Parent)
		parentRepoDir := filepath.Join(gitDir, fmt.Sprintf("%d.git", repository.Parent))
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

	// Run git gc
	cmd = exec.Command("git", "gc")
	cmd.Dir = repoDir
	if err := cmd.Run(); err != nil {
		return errors.Wrapf(err, "error running git gc for repo %d", repoID)
	}

	// For forked repos with alternates, run git repack to remove common objects
	if repository.Fork {
		alternatesPath := filepath.Join(repoDir, "objects", "info", "alternates")
		if _, err := os.Stat(alternatesPath); err == nil {
			fmt.Printf("Running git repack to optimize forked repository %d\n", repoID)
			cmd = exec.Command("git", "repack", "-a", "-d", "-l")
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
	} else if repository.Fork {
		fmt.Printf("Forked repository %d has no packfile (no new changes), using parent objects via alternates\n", repoID)
	}

	// Process LFS objects
	if err := processLFSObjects(ctx, repoID, repoDir, ipfsClusterClient, storageManager); err != nil {
		return errors.Wrapf(err, "error processing LFS objects for repo %d", repoID)
	}

	return nil
}

func processLFSObjects(ctx context.Context, repositoryId uint64, repoDir string, ipfsClusterClient ipfsclusterclient.Client, storageManager *shared.StorageManager) error {
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
			progress, err := loadProgress()
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

			// Process repositories
			var processedCount int
			var totalRepositories uint64
			nextKey := progress.RepositoryNextKey

			for {
				repositories, err := gitopiaClient.QueryClient().Gitopia.RepositoryAll(ctx, &gitopiatypes.QueryAllRepositoryRequest{
					Pagination: &query.PageRequest{
						Key: nextKey,
					},
				})
				if err != nil {
					return errors.Wrap(err, "failed to get repositories")
				}

				if totalRepositories == 0 {
					totalRepositories = repositories.Pagination.Total
					fmt.Printf("Total repositories to process: %d\n", totalRepositories)
				}

				// Track current batch for better recovery
				var currentBatchRepos []uint64
				for _, repo := range repositories.Repository {
					currentBatchRepos = append(currentBatchRepos, repo.Id)
				}
				progress.CurrentBatchRepos = currentBatchRepos
				if err := saveProgress(progress); err != nil {
					return errors.Wrap(err, "failed to save batch progress")
				}

				for _, repository := range repositories.Repository {
					// Use improved resume logic
					if !shouldProcessRepo(progress, repository.Id) {
						processedCount++
						continue
					}

					// Validate fork dependencies
					if err := validateForkDependency(progress, repository, gitDir); err != nil {
						fmt.Printf("Skipping fork repository %d due to dependency issue: %v\n", repository.Id, err)
						processedCount++
						continue
					}

					fmt.Printf("Processing repository %d (%d/%d)\n", repository.Id, processedCount+1, totalRepositories)

					// Use atomic processing function
					if err := processRepositoryAtomic(ctx, repository, gitDir, ipfsClusterClient, storageManager, &gitopiaClient, progress); err != nil {
						progress.FailedRepos[repository.Id] = err.Error()
						progress.LastFailedRepo = repository.Id
						if err := saveProgress(progress); err != nil {
							return errors.Wrap(err, "failed to save progress")
						}
						return errors.Wrapf(err, "error processing repository %d", repository.Id)
					}

					// Mark repository as successfully processed
					delete(progress.FailedRepos, repository.Id)
					progress.ProcessedRepos[repository.Id] = true
					progress.LastProcessedRepoID = repository.Id
					progress.RepositoryNextKey = nextKey
					if err := saveProgress(progress); err != nil {
						return errors.Wrap(err, "failed to save progress")
					}

					// Save storage manager state
					if err := storageManager.Save(); err != nil {
						return errors.Wrap(err, "failed to save storage manager")
					}

					processedCount++
					fmt.Printf("Successfully cloned repository %d\n", repository.Id)
				}

				if repositories.Pagination == nil || len(repositories.Pagination.NextKey) == 0 {
					break
				}
				nextKey = repositories.Pagination.NextKey
			}

			// Process releases
			processedCount = 0
			var totalReleases uint64
			nextKey = progress.ReleaseNextKey

			for {
				releases, err := gitopiaClient.QueryClient().Gitopia.ReleaseAll(ctx, &gitopiatypes.QueryAllReleaseRequest{
					Pagination: &query.PageRequest{
						Key: nextKey,
					},
				})
				if err != nil {
					return errors.Wrap(err, "failed to get releases")
				}

				if totalReleases == 0 {
					totalReleases = releases.Pagination.Total
					fmt.Printf("Total releases to process: %d\n", totalReleases)
				}

				// Track current batch for better recovery
				var currentBatchReleases []uint64
				for _, rel := range releases.Release {
					currentBatchReleases = append(currentBatchReleases, rel.Id)
				}
				progress.CurrentBatchReleases = currentBatchReleases
				if err := saveProgress(progress); err != nil {
					return errors.Wrap(err, "failed to save batch progress")
				}

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
						if err := saveProgress(progress); err != nil {
							return errors.Wrap(err, "failed to save progress")
						}
						return errors.Wrap(err, "error getting repository")
					}

					for _, attachment := range release.Attachments {
						// Download release asset
						attachmentUrl := fmt.Sprintf("%s/releases/%s/%s/%s/%s",
							viper.GetString("GIT_SERVER_HOST"),
							repository.Repository.Owner.Id,
							repository.Repository.Name,
							release.TagName,
							attachment.Name)

						filePath := filepath.Join(attachmentDir, attachment.Sha)
						cmd := exec.Command("wget", attachmentUrl, "-O", filePath)
						output, err := cmd.CombinedOutput()
						if err != nil {
							progress.FailedReleases[release.Id] = err.Error()
							progress.LastFailedRelease = release.Id
							if err := saveProgress(progress); err != nil {
								return errors.Wrap(err, "failed to save progress")
							}
							return errors.Wrapf(err, "error downloading release asset: %s", string(output))
						}

						// Verify sha256
						cmd = exec.Command("sha256sum", filePath)
						output, err = cmd.Output()
						if err != nil {
							progress.FailedReleases[release.Id] = err.Error()
							progress.LastFailedRelease = release.Id
							if err := saveProgress(progress); err != nil {
								return errors.Wrap(err, "failed to save progress")
							}
							return errors.Wrap(err, "error verifying sha256 for attachment")
						}
						calculatedHash := strings.Fields(string(output))[0]
						if calculatedHash != attachment.Sha {
							err := errors.Errorf("SHA256 mismatch for attachment %s: %s != %s", attachment.Name, calculatedHash, attachment.Sha)
							progress.FailedReleases[release.Id] = err.Error()
							progress.LastFailedRelease = release.Id
							if err := saveProgress(progress); err != nil {
								return errors.Wrap(err, "failed to save progress")
							}
							return err
						}

						// Pin to IPFS cluster
						cid, err := shared.PinFile(ipfsClusterClient, filePath)
						if err != nil {
							progress.FailedReleases[release.Id] = err.Error()
							progress.LastFailedRelease = release.Id
							if err := saveProgress(progress); err != nil {
								return errors.Wrap(err, "failed to save progress")
							}
							return errors.Wrap(err, "error pinning attachment")
						}

						// Compute merkle root and file info
						rootHash, size, err := shared.ComputeFileInfo(filePath, cid)
						if err != nil {
							progress.FailedReleases[release.Id] = err.Error()
							progress.LastFailedRelease = release.Id
							if err := saveProgress(progress); err != nil {
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
					}

					// Mark release as successfully processed
					delete(progress.FailedReleases, release.Id)
					progress.ProcessedReleases[release.Id] = true
					progress.LastProcessedReleaseID = release.Id
					progress.ReleaseNextKey = nextKey
					if err := saveProgress(progress); err != nil {
						return errors.Wrap(err, "failed to save progress")
					}

					// Save storage manager state
					if err := storageManager.Save(); err != nil {
						return errors.Wrap(err, "failed to save storage manager")
					}

					processedCount++
				}

				if releases.Pagination == nil || len(releases.Pagination.NextKey) == 0 {
					break
				}
				nextKey = releases.Pagination.NextKey
			}

			fmt.Println("Clone script completed successfully!")
			return nil
		},
	}

	// Add flags
	rootCmd.Flags().String("from", "", "Name or address of private key with which to sign")
	rootCmd.Flags().String("keyring-backend", "", "Select keyring's backend (os|file|kwallet|pass|test|memory)")

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
