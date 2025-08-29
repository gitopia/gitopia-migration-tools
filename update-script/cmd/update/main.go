package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
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
	gitopiatypes "github.com/gitopia/gitopia/v6/x/gitopia/types"
	storagetypes "github.com/gitopia/gitopia/v6/x/storage/types"
	"github.com/ipfs/boxo/files"
	ipfspath "github.com/ipfs/boxo/path"
	"github.com/ipfs/kubo/client/rpc"
	"github.com/pkg/errors"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

const (
	AccountAddressPrefix = "gitopia"
	AccountPubKeyPrefix  = AccountAddressPrefix + sdk.PrefixPublic
	AppName              = "update-script"
	ProgressFile         = "update_progress.json"
	GAS_ADJUSTMENT       = 1.5
	BLOCK_TIME           = 6 * time.Second
)

type UpdateProgress struct {
	RepositoryNextKey []byte            `json:"repository_next_key"`
	ReleaseNextKey    []byte            `json:"release_next_key"`
	FailedRepos       map[uint64]string `json:"failed_repos"`
	FailedReleases    map[uint64]string `json:"failed_releases"`
	LastFailedRepo    uint64            `json:"last_failed_repo"`
	LastFailedRelease uint64            `json:"last_failed_release"`
}

type GitopiaProxy struct {
	client gc.Client
}

func NewGitopiaProxy(client gc.Client) *GitopiaProxy {
	return &GitopiaProxy{client: client}
}

func (gp *GitopiaProxy) UpdateRepositoryPackfile(ctx context.Context, repositoryId uint64, name, cid string, rootHash []byte, size int64, oldCid string) error {
	msg := &storagetypes.MsgUpdatePackfile{
		Creator:      gp.client.ClientAddress(),
		RepositoryId: repositoryId,
		Name:         name,
		Cid:          cid,
		RootHash:     rootHash,
		Size_:        uint64(size),
		OldCid:       oldCid,
	}

	_, err := gp.client.BroadcastTx(ctx, msg)
	return err
}

func (gp *GitopiaProxy) UpdateReleaseAssets(ctx context.Context, repositoryId uint64, tagName string, assets []*storagetypes.ReleaseAssetUpdate) error {
	msg := &storagetypes.MsgUpdateReleaseAssets{
		Creator:      gp.client.ClientAddress(),
		RepositoryId: repositoryId,
		TagName:      tagName,
		Assets:       assets,
	}

	_, err := gp.client.BroadcastTx(ctx, msg)
	return err
}

func (gp *GitopiaProxy) UpdateLFSObject(ctx context.Context, repositoryId uint64, oid, cid string, rootHash []byte, size int64) error {
	msg := &storagetypes.MsgUpdateLFSObject{
		Creator:      gp.client.ClientAddress(),
		RepositoryId: repositoryId,
		Oid:          oid,
		Cid:          cid,
		RootHash:     rootHash,
		Size_:        uint64(size),
	}

	_, err := gp.client.BroadcastTx(ctx, msg)
	return err
}

func (gp *GitopiaProxy) PollForUpdate(ctx context.Context, checkFunc func() (bool, error)) error {
	timeout := time.After(2 * time.Minute)
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-timeout:
			return context.DeadlineExceeded
		case <-ticker.C:
			success, err := checkFunc()
			if err != nil {
				return err
			}
			if success {
				return nil
			}
		}
	}
}

func (gp *GitopiaProxy) RepositoryPackfile(ctx context.Context, repositoryId uint64) (*storagetypes.RepositoryPackfile, error) {
	resp, err := gp.client.QueryClient().Storage.RepositoryPackfile(ctx, &storagetypes.QueryGetRepositoryPackfileRequest{
		RepositoryId: repositoryId,
	})
	if err != nil {
		return nil, err
	}
	return &resp.RepositoryPackfile, nil
}

func (gp *GitopiaProxy) LFSObjectByRepositoryIdAndOid(ctx context.Context, repositoryId uint64, oid string) (*storagetypes.LFSObject, error) {
	resp, err := gp.client.QueryClient().Storage.LFSObject(ctx, &storagetypes.QueryGetLFSObjectRequest{
		RepositoryId: repositoryId,
		Oid:          oid,
	})
	if err != nil {
		return nil, err
	}
	return &resp.LFSObject, nil
}

func (gp *GitopiaProxy) RepositoryReleaseAssets(ctx context.Context, repositoryId uint64, tagName string) ([]storagetypes.ReleaseAsset, error) {
	resp, err := gp.client.QueryClient().Storage.ReleaseAssets(ctx, &storagetypes.QueryGetReleaseAssetsRequest{
		RepositoryId: repositoryId,
		TagName:      tagName,
	})
	if err != nil {
		return nil, err
	}
	return resp.ReleaseAssets.Assets, nil
}

func loadProgress() (*UpdateProgress, error) {
	if _, err := os.Stat(ProgressFile); os.IsNotExist(err) {
		return &UpdateProgress{
			FailedRepos:    make(map[uint64]string),
			FailedReleases: make(map[uint64]string),
		}, nil
	}

	data, err := os.ReadFile(ProgressFile)
	if err != nil {
		return nil, err
	}

	var progress UpdateProgress
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

func saveProgress(progress *UpdateProgress) error {
	data, err := json.Marshal(progress)
	if err != nil {
		return errors.Wrap(err, "failed to marshal progress")
	}
	return os.WriteFile(ProgressFile, data, 0644)
}

func computeMerkleRoot(file files.File) ([]byte, error) {
	// This is a simplified merkle root calculation
	// In production, you should use the actual merkleproof package
	// from gitopia-storage
	return []byte("mock_merkle_root"), nil
}

func getPackfileName(repoDir string) (string, error) {
	packDir := filepath.Join(repoDir, "objects", "pack")
	packFiles, err := filepath.Glob(filepath.Join(packDir, "*.pack"))
	if err != nil {
		return "", err
	}
	if len(packFiles) == 0 {
		return "", errors.New("no packfiles found")
	}
	return packFiles[0], nil
}

func processLFSObjects(ctx context.Context, repositoryId uint64, repoDir string, ipfsHttpApi *rpc.HttpApi, gitopiaProxy *GitopiaProxy) error {
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

		// Get the CID from IPFS cluster (assuming it was already pinned)
		// In a real implementation, you would query the IPFS cluster for the CID
		cid := "mock_cid_for_" + oid

		// Get LFS object from IPFS and calculate merkle root
		p, err := ipfspath.NewPath("/ipfs/" + cid)
		if err != nil {
			return errors.Wrapf(err, "error creating IPFS path for LFS object %s", oid)
		}

		f, err := ipfsHttpApi.Unixfs().Get(ctx, p)
		if err != nil {
			return errors.Wrapf(err, "error getting LFS object from IPFS: %s", oid)
		}

		file, ok := f.(files.File)
		if !ok {
			return errors.Errorf("invalid LFS object format: %s", oid)
		}

		rootHash, err := computeMerkleRoot(file)
		if err != nil {
			return errors.Wrapf(err, "error computing merkle root for LFS object %s", oid)
		}

		// Get LFS object size
		oidPath := filepath.Join(lfsObjectsDir, oid[:2], oid[2:])
		lfsObjectInfo, err := os.Stat(oidPath)
		if err != nil {
			return errors.Wrapf(err, "error getting LFS object size: %s", oid)
		}

		// Update LFS object on chain
		err = gitopiaProxy.UpdateLFSObject(
			ctx,
			repositoryId,
			oid,
			cid,
			rootHash,
			lfsObjectInfo.Size(),
		)
		if err != nil {
			return errors.Wrapf(err, "error updating LFS object %s for repo %d", oid, repositoryId)
		}

		// Poll to check if the LFS object was updated
		fmt.Printf("Verifying LFS object update for oid %s...\n", oid)
		err = gitopiaProxy.PollForUpdate(ctx, func() (bool, error) {
			lfsObject, err := gitopiaProxy.LFSObjectByRepositoryIdAndOid(ctx, repositoryId, oid)
			if err != nil {
				return false, err
			}
			return lfsObject.Cid == cid, nil
		})
		if err != nil {
			return errors.Wrapf(err, "failed to verify LFS object update for oid %s", oid)
		}

		fmt.Printf("Successfully processed LFS object %s (CID: %s)\n", oid, cid)
	}

	return nil
}

func main() {
	rootCmd := &cobra.Command{
		Use:               "update-script",
		Short:             "Update packfiles, LFS objects and release assets on chain after upgrade",
		CompletionOptions: cobra.CompletionOptions{DisableDefaultCmd: true},
		PersistentPreRunE: func(cmd *cobra.Command, _ []string) error {
			return gc.CommandInit(cmd, AppName)
		},
		RunE: func(cmd *cobra.Command, args []string) error {
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
			txf = txf.WithGasAdjustment(GAS_ADJUSTMENT)

			gitopiaClient, err := gc.NewClient(ctx, clientCtx, txf)
			if err != nil {
				return err
			}
			defer gitopiaClient.Close()

			gitopiaProxy := NewGitopiaProxy(gitopiaClient)

			// Initialize IPFS HTTP API client
			ipfsHttpApi, err := rpc.NewURLApiWithClient(
				fmt.Sprintf("http://%s:%s", viper.GetString("IPFS_HOST"), viper.GetString("IPFS_PORT")),
				&http.Client{},
			)
			if err != nil {
				return errors.Wrap(err, "failed to create IPFS API")
			}

			gitDir := viper.GetString("GIT_REPOS_DIR")

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

					fmt.Printf("Processing repository %d (%d/%d)\n", repository.Id, processedCount+1, totalRepositories)

					repoDir := filepath.Join(gitDir, fmt.Sprintf("%d.git", repository.Id))

					// Check if repository directory exists
					if _, err := os.Stat(repoDir); os.IsNotExist(err) {
						fmt.Printf("Repository %d not found locally, skipping\n", repository.Id)
						processedCount++
						continue
					}

					// Get packfile path
					packfileName, err := getPackfileName(repoDir)
					if err != nil {
						progress.FailedRepos[repository.Id] = err.Error()
						progress.LastFailedRepo = repository.Id
						if err := saveProgress(progress); err != nil {
							return errors.Wrap(err, "failed to save progress")
						}
						return errors.Wrapf(err, "error getting packfile for repo %d", repository.Id)
					}

					// Get packfile CID from IPFS cluster (assuming it was already pinned)
					// In a real implementation, you would query the IPFS cluster for the CID
					cid := "mock_cid_for_repo_" + fmt.Sprintf("%d", repository.Id)

					// Get packfile from IPFS and calculate merkle root
					p, err := ipfspath.NewPath("/ipfs/" + cid)
					if err != nil {
						progress.FailedRepos[repository.Id] = err.Error()
						progress.LastFailedRepo = repository.Id
						if err := saveProgress(progress); err != nil {
							return errors.Wrap(err, "failed to save progress")
						}
						return errors.Wrapf(err, "error creating IPFS path for repo %d", repository.Id)
					}

					f, err := ipfsHttpApi.Unixfs().Get(ctx, p)
					if err != nil {
						progress.FailedRepos[repository.Id] = err.Error()
						progress.LastFailedRepo = repository.Id
						if err := saveProgress(progress); err != nil {
							return errors.Wrap(err, "failed to save progress")
						}
						return errors.Wrapf(err, "error getting packfile from IPFS for repo %d", repository.Id)
					}

					file, ok := f.(files.File)
					if !ok {
						err := errors.Errorf("invalid packfile format for repo %d", repository.Id)
						progress.FailedRepos[repository.Id] = err.Error()
						progress.LastFailedRepo = repository.Id
						if err := saveProgress(progress); err != nil {
							return errors.Wrap(err, "failed to save progress")
						}
						return err
					}

					rootHash, err := computeMerkleRoot(file)
					if err != nil {
						progress.FailedRepos[repository.Id] = err.Error()
						progress.LastFailedRepo = repository.Id
						if err := saveProgress(progress); err != nil {
							return errors.Wrap(err, "failed to save progress")
						}
						return errors.Wrapf(err, "error computing merkle root for repo %d", repository.Id)
					}

					// Get packfile size
					packfileInfo, err := os.Stat(packfileName)
					if err != nil {
						progress.FailedRepos[repository.Id] = err.Error()
						progress.LastFailedRepo = repository.Id
						if err := saveProgress(progress); err != nil {
							return errors.Wrap(err, "failed to save progress")
						}
						return errors.Wrapf(err, "error getting packfile size for repo %d", repository.Id)
					}

					// Update repository packfile on chain
					err = gitopiaProxy.UpdateRepositoryPackfile(
						ctx,
						repository.Id,
						filepath.Base(packfileName),
						cid,
						rootHash,
						packfileInfo.Size(),
						"", // old CID - empty for initial update
					)
					if err != nil {
						progress.FailedRepos[repository.Id] = err.Error()
						progress.LastFailedRepo = repository.Id
						if err := saveProgress(progress); err != nil {
							return errors.Wrap(err, "failed to save progress")
						}
						return errors.Wrapf(err, "error updating repository packfile for repo %d", repository.Id)
					}

					// Poll to check if the packfile was updated
					fmt.Printf("Verifying packfile update for repository %d...\n", repository.Id)
					err = gitopiaProxy.PollForUpdate(ctx, func() (bool, error) {
						packfile, err := gitopiaProxy.RepositoryPackfile(ctx, repository.Id)
						if err != nil {
							return false, err
						}
						return packfile.Cid == cid, nil
					})
					if err != nil {
						err = errors.Wrapf(err, "failed to verify packfile update for repo %d", repository.Id)
						progress.FailedRepos[repository.Id] = err.Error()
						progress.LastFailedRepo = repository.Id
						if err := saveProgress(progress); err != nil {
							return errors.Wrap(err, "failed to save progress")
						}
						return err
					}
					fmt.Printf("Packfile update for repository %d verified.\n", repository.Id)

					// Process LFS objects
					if err := processLFSObjects(ctx, repository.Id, repoDir, ipfsHttpApi, gitopiaProxy); err != nil {
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
					fmt.Printf("Successfully updated repository %d\n", repository.Id)
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
			attachmentDir := viper.GetString("ATTACHMENT_DIR")

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

					assets := make([]*storagetypes.ReleaseAssetUpdate, 0)
					for _, attachment := range release.Attachments {
						filePath := filepath.Join(attachmentDir, attachment.Sha)

						// Check if file exists
						if _, err := os.Stat(filePath); os.IsNotExist(err) {
							fmt.Printf("Attachment file %s not found, skipping\n", attachment.Name)
							continue
						}

						// Get CID from IPFS cluster (assuming it was already pinned)
						cid := "mock_cid_for_attachment_" + attachment.Sha

						// Open the file for merkle root calculation
						file, err := os.Open(filePath)
						if err != nil {
							progress.FailedReleases[release.Id] = err.Error()
							progress.LastFailedRelease = release.Id
							if err := saveProgress(progress); err != nil {
								return errors.Wrap(err, "failed to save progress")
							}
							return errors.Wrap(err, "error opening attachment file")
						}
						defer file.Close()

						stat, err := file.Stat()
						if err != nil {
							progress.FailedReleases[release.Id] = err.Error()
							progress.LastFailedRelease = release.Id
							if err := saveProgress(progress); err != nil {
								return errors.Wrap(err, "failed to save progress")
							}
							return errors.Wrap(err, "error getting file stat")
						}

						// Create a files.File from the os.File
						ipfsFile, err := files.NewReaderPathFile(filePath, file, stat)
						if err != nil {
							progress.FailedReleases[release.Id] = err.Error()
							progress.LastFailedRelease = release.Id
							if err := saveProgress(progress); err != nil {
								return errors.Wrap(err, "failed to save progress")
							}
							return errors.Wrap(err, "error creating files.File from attachment file")
						}

						// Calculate merkle root
						rootHash, err := computeMerkleRoot(ipfsFile)
						if err != nil {
							progress.FailedReleases[release.Id] = err.Error()
							progress.LastFailedRelease = release.Id
							if err := saveProgress(progress); err != nil {
								return errors.Wrap(err, "failed to save progress")
							}
							return errors.Wrap(err, "error computing merkle root for attachment")
						}

						// Get file size
						fileInfo, err := file.Stat()
						if err != nil {
							progress.FailedReleases[release.Id] = err.Error()
							progress.LastFailedRelease = release.Id
							if err := saveProgress(progress); err != nil {
								return errors.Wrap(err, "failed to save progress")
							}
							return errors.Wrap(err, "error getting file size")
						}

						// Collect release asset for batch update
						asset := &storagetypes.ReleaseAssetUpdate{
							Name:     attachment.Name,
							Cid:      cid,
							RootHash: rootHash,
							Size_:    uint64(fileInfo.Size()),
							Sha256:   attachment.Sha,
						}
						assets = append(assets, asset)

						fmt.Printf("Prepared attachment %s for release %s\n", attachment.Name, release.TagName)
					}

					if len(assets) > 0 {
						err = gitopiaProxy.UpdateReleaseAssets(ctx, release.RepositoryId, release.TagName, assets)
						if err != nil {
							progress.FailedReleases[release.Id] = err.Error()
							progress.LastFailedRelease = release.Id
							if err := saveProgress(progress); err != nil {
								return errors.Wrap(err, "failed to save progress")
							}
							return errors.Wrap(err, "error updating release assets")
						}

						// Poll to check if the release assets were updated
						fmt.Printf("Verifying release assets update for release %s...\n", release.TagName)
						err = gitopiaProxy.PollForUpdate(ctx, func() (bool, error) {
							updatedAssets, err := gitopiaProxy.RepositoryReleaseAssets(ctx, release.RepositoryId, release.TagName)
							if err != nil {
								return false, err
							}

							if len(updatedAssets) != len(assets) {
								return false, nil
							}

							// Create a map of expected assets for easy lookup
							expectedAssets := make(map[string]*storagetypes.ReleaseAssetUpdate)
							for _, asset := range assets {
								expectedAssets[asset.Name] = asset
							}

							for _, updatedAsset := range updatedAssets {
								expected := expectedAssets[updatedAsset.Name]
								if updatedAsset.Cid != expected.Cid {
									return false, nil
								}
							}

							return true, nil
						})
						if err != nil {
							err = errors.Wrapf(err, "failed to verify release assets update for release %s", release.TagName)
							progress.FailedReleases[release.Id] = err.Error()
							progress.LastFailedRelease = release.Id
							if err := saveProgress(progress); err != nil {
								return errors.Wrap(err, "failed to save progress")
							}
							return err
						}
						fmt.Printf("Release assets update for release %s verified.\n", release.TagName)
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

			fmt.Println("Update script completed successfully!")
			return nil
		},
	}

	// Add flags
	rootCmd.Flags().String("from", "", "Name or address of private key with which to sign")
	rootCmd.Flags().String("keyring-backend", "", "Select keyring's backend (os|file|kwallet|pass|test|memory)")
	rootCmd.Flags().String("fees", "", "Fees to pay along with transaction; eg: 10ulore")

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
