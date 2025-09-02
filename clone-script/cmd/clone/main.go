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
	"github.com/gitopia/gitopia-migration-tools/utils"
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
	RepositoryNextKey []byte            `json:"repository_next_key"`
	ReleaseNextKey    []byte            `json:"release_next_key"`
	FailedRepos       map[uint64]string `json:"failed_repos"`
	FailedReleases    map[uint64]string `json:"failed_releases"`
	LastFailedRepo    uint64            `json:"last_failed_repo"`
	LastFailedRelease uint64            `json:"last_failed_release"`
}

func loadProgress() (*CloneProgress, error) {
	if _, err := os.Stat(ProgressFile); os.IsNotExist(err) {
		return &CloneProgress{
			FailedRepos:    make(map[uint64]string),
			FailedReleases: make(map[uint64]string),
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

	return &progress, nil
}

func saveProgress(progress *CloneProgress) error {
	data, err := json.Marshal(progress)
	if err != nil {
		return errors.Wrap(err, "failed to marshal progress")
	}
	return os.WriteFile(ProgressFile, data, 0644)
}

func processLFSObjects(ctx context.Context, repositoryId uint64, repoDir string, ipfsClusterClient ipfsclusterclient.Client) error {
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

		_, err := utils.PinFileSimple(ipfsClusterClient, oidPath)
		if err != nil {
			return errors.Wrapf(err, "error pinning LFS object %s", oid)
		}

		fmt.Printf("Successfully pinned LFS object %s\n", oid)
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
			resumeFromFailed := progress.LastFailedRepo > 0

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

				for _, repository := range repositories.Repository {
					if resumeFromFailed && repository.Id < progress.LastFailedRepo {
						processedCount++
						continue
					}
					resumeFromFailed = false

					// Check if repository is empty
					branch, err := gitopiaClient.QueryClient().Gitopia.RepositoryBranch(ctx, &gitopiatypes.QueryGetRepositoryBranchRequest{
						Id:             repository.Owner.Id,
						RepositoryName: repository.Name,
						BranchName:     repository.DefaultBranch,
					})
					if err != nil {
						progress.FailedRepos[repository.Id] = err.Error()
						progress.LastFailedRepo = repository.Id
						if err := saveProgress(progress); err != nil {
							return errors.Wrap(err, "failed to save progress")
						}
						return errors.Wrapf(err, "error getting repository branches for repo %d", repository.Id)
					}
					if branch.Branch.Name == "" {
						fmt.Printf("Repository %d is empty, skipping\n", repository.Id)
						processedCount++
						continue
					}

					fmt.Printf("Processing repository %d (%d/%d)\n", repository.Id, processedCount+1, totalRepositories)

					// Clone repository
					repoDir := filepath.Join(gitDir, fmt.Sprintf("%d.git", repository.Id))
					remoteUrl := fmt.Sprintf("gitopia://%s/%s", repository.Owner.Id, repository.Name)
					cmd := exec.Command("git", "clone", "--bare", remoteUrl, repoDir)
					if err := cmd.Run(); err != nil {
						progress.FailedRepos[repository.Id] = err.Error()
						progress.LastFailedRepo = repository.Id
						if err := saveProgress(progress); err != nil {
							return errors.Wrap(err, "failed to save progress")
						}
						return errors.Wrapf(err, "error cloning repository %d", repository.Id)
					}

					// Handle forked repository optimization
					if repository.Fork {
						fmt.Printf("Repository %d is a fork of %d, setting up alternates\n", repository.Id, repository.Parent)

						// Ensure parent repository is cloned first
						parentRepoDir := filepath.Join(gitDir, fmt.Sprintf("%d.git", repository.Parent))
						if _, err := os.Stat(parentRepoDir); os.IsNotExist(err) {
							fmt.Printf("Parent repository %d not found, will be processed later\n", repository.Parent)
						} else {
							// Create alternates file to link with parent repo
							alternatesPath := filepath.Join(repoDir, "objects", "info", "alternates")
							if err := os.MkdirAll(filepath.Dir(alternatesPath), 0755); err != nil {
								progress.FailedRepos[repository.Id] = err.Error()
								progress.LastFailedRepo = repository.Id
								if err := saveProgress(progress); err != nil {
									return errors.Wrap(err, "failed to save progress")
								}
								return errors.Wrapf(err, "error creating alternates directory for repo %d", repository.Id)
							}

							parentObjectsPath := filepath.Join(parentRepoDir, "objects")
							if err := os.WriteFile(alternatesPath, []byte(parentObjectsPath+"\n"), 0644); err != nil {
								progress.FailedRepos[repository.Id] = err.Error()
								progress.LastFailedRepo = repository.Id
								if err := saveProgress(progress); err != nil {
									return errors.Wrap(err, "failed to save progress")
								}
								return errors.Wrapf(err, "error creating alternates file for repo %d", repository.Id)
							}
							fmt.Printf("Created alternates file linking to parent repository %d\n", repository.Parent)
						}
					}

					// Run git gc
					cmd = exec.Command("git", "gc")
					cmd.Dir = repoDir
					if err := cmd.Run(); err != nil {
						progress.FailedRepos[repository.Id] = err.Error()
						progress.LastFailedRepo = repository.Id
						if err := saveProgress(progress); err != nil {
							return errors.Wrap(err, "failed to save progress")
						}
						return errors.Wrapf(err, "error running git gc for repo %d", repository.Id)
					}

					// For forked repos with alternates, run git repack to remove common objects
					if repository.Fork {
						alternatesPath := filepath.Join(repoDir, "objects", "info", "alternates")
						if _, err := os.Stat(alternatesPath); err == nil {
							fmt.Printf("Running git repack to optimize forked repository %d\n", repository.Id)
							cmd = exec.Command("git", "repack", "-a", "-d", "-l")
							cmd.Dir = repoDir
							if err := cmd.Run(); err != nil {
								fmt.Printf("Warning: git repack failed for forked repo %d: %v\n", repository.Id, err)
							}
						}
					}

					// Pin packfile to IPFS cluster (skip for forked repos with no new objects)
					packDir := filepath.Join(repoDir, "objects", "pack")
					packFiles, err := filepath.Glob(filepath.Join(packDir, "*.pack"))
					if err != nil {
						progress.FailedRepos[repository.Id] = err.Error()
						progress.LastFailedRepo = repository.Id
						if err := saveProgress(progress); err != nil {
							return errors.Wrap(err, "failed to save progress")
						}
						return errors.Wrapf(err, "error finding packfiles for repo %d", repository.Id)
					}

					// Only pin packfiles for non-forked repos or forked repos with new objects
					if len(packFiles) > 0 {
						packfileName := packFiles[0] // Use the first packfile
						_, err := utils.PinFileSimple(ipfsClusterClient, packfileName)
						if err != nil {
							progress.FailedRepos[repository.Id] = err.Error()
							progress.LastFailedRepo = repository.Id
							if err := saveProgress(progress); err != nil {
								return errors.Wrap(err, "failed to save progress")
							}
							return errors.Wrapf(err, "error pinning packfile for repo %d", repository.Id)
						}
						fmt.Printf("Successfully pinned packfile for repository %d\n", repository.Id)
					} else if repository.Fork {
						fmt.Printf("Forked repository %d has no packfile (no new changes), using parent objects via alternates\n", repository.Id)
					}

					// Process LFS objects
					if err := processLFSObjects(ctx, repository.Id, repoDir, ipfsClusterClient); err != nil {
						progress.FailedRepos[repository.Id] = err.Error()
						progress.LastFailedRepo = repository.Id
						if err := saveProgress(progress); err != nil {
							return errors.Wrap(err, "failed to save progress")
						}
						return errors.Wrapf(err, "error processing LFS objects for repo %d", repository.Id)
					}

					// Remove from failed repos if it was previously failed
					delete(progress.FailedRepos, repository.Id)
					progress.RepositoryNextKey = nextKey
					if err := saveProgress(progress); err != nil {
						return errors.Wrap(err, "failed to save progress")
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
			resumeFromFailed = progress.LastFailedRelease > 0

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

				for _, release := range releases.Release {
					if resumeFromFailed && release.Id < progress.LastFailedRelease {
						processedCount++
						continue
					}
					resumeFromFailed = false

					fmt.Printf("Processing release %s for repository %d (%d/%d)\n", release.TagName, release.RepositoryId, processedCount+1, totalReleases)

					if len(release.Attachments) == 0 {
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
						_, err = utils.PinFileSimple(ipfsClusterClient, filePath)
						if err != nil {
							progress.FailedReleases[release.Id] = err.Error()
							progress.LastFailedRelease = release.Id
							if err := saveProgress(progress); err != nil {
								return errors.Wrap(err, "failed to save progress")
							}
							return errors.Wrap(err, "error pinning attachment")
						}

						fmt.Printf("Successfully downloaded and pinned attachment %s for release %s\n", attachment.Name, release.TagName)
					}

					// Remove from failed releases if it was previously failed
					delete(progress.FailedReleases, release.Id)
					progress.ReleaseNextKey = nextKey
					if err := saveProgress(progress); err != nil {
						return errors.Wrap(err, "failed to save progress")
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
