package main

import (
	"context"
	"fmt"
	"os"
	"strings"

	"github.com/cosmos/cosmos-sdk/client"
	"github.com/cosmos/cosmos-sdk/client/keys"
	"github.com/cosmos/cosmos-sdk/client/tx"
	sdk "github.com/cosmos/cosmos-sdk/types"
	gc "github.com/gitopia/gitopia-go"
	"github.com/gitopia/gitopia-go/logger"
	storagetypes "github.com/gitopia/gitopia/v6/x/storage/types"
	"github.com/pkg/errors"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"

	"github.com/gitopia/gitopia-migration-tools/shared"
)

const (
	AccountAddressPrefix = "gitopia"
	AccountPubKeyPrefix  = AccountAddressPrefix + sdk.PrefixPublic
	AppName              = "update-script"
	BATCH_SIZE           = 10 // Process 10 messages per block
)

type BatchTxManager struct {
	client    gc.Client
	txQueue   []sdk.Msg
	batchSize int
}

func NewBatchTxManager(client gc.Client, batchSize int) *BatchTxManager {
	return &BatchTxManager{
		client:    client,
		txQueue:   make([]sdk.Msg, 0, batchSize),
		batchSize: batchSize,
	}
}

func (btm *BatchTxManager) AddToBatch(ctx context.Context, msg sdk.Msg) error {
	btm.txQueue = append(btm.txQueue, msg)

	// Process batch when it reaches the batch size
	if len(btm.txQueue) >= btm.batchSize {
		return btm.ProcessBatch(ctx)
	}
	return nil
}

func (btm *BatchTxManager) ProcessBatch(ctx context.Context) error {
	if len(btm.txQueue) == 0 {
		return nil
	}

	fmt.Printf("Processing batch of %d transactions...\n", len(btm.txQueue))

	// Send all messages in a single transaction
	err := btm.client.BroadcastTxAndWait(ctx, btm.txQueue...)
	if err != nil {
		return errors.Wrap(err, "failed to broadcast batch transaction")
	}

	fmt.Printf("Successfully processed batch of %d transactions\n", len(btm.txQueue))

	// Clear the queue
	btm.txQueue = btm.txQueue[:0]
	return nil
}

func (btm *BatchTxManager) FlushBatch(ctx context.Context) error {
	return btm.ProcessBatch(ctx)
}

// queryChainPackfile queries the packfile information from the chain
func queryChainPackfile(ctx context.Context, client gc.Client, repositoryId uint64) (storagetypes.Packfile, error) {
	resp, err := client.QueryClient().Storage.RepositoryPackfile(ctx, &storagetypes.QueryRepositoryPackfileRequest{
		RepositoryId: repositoryId,
	})
	if err != nil {
		return storagetypes.Packfile{}, err
	}
	return resp.Packfile, nil
}

// isPackfileNotFoundError checks if the error indicates packfile not found
func isPackfileNotFoundError(err error) bool {
	return strings.Contains(err.Error(), "packfile not found")
}

// processRepositoryPackfiles processes repository packfiles using stored data
// with chain query optimization - only updates if CID differs or doesn't exist
func processRepositoryPackfiles(ctx context.Context, batchMgr *BatchTxManager, storageManager *shared.StorageManager) error {
	allPackfiles := storageManager.GetAllPackfileInfo()

	if allPackfiles == nil {
		fmt.Println("No packfile data retrieved (could be empty or error)")
		return nil
	}

	fmt.Printf("Found %d packfiles to process\n", len(allPackfiles))

	processed := 0
	skipped := 0

	for _, packfileInfo := range allPackfiles {
		// Query packfile information from chain
		chainPackfile, err := queryChainPackfile(ctx, batchMgr.client, packfileInfo.RepositoryID)
		if err != nil {
			// If packfile doesn't exist on chain, we should update it
			if isPackfileNotFoundError(err) {
				fmt.Printf("Packfile for repository %d not found on chain, will update\n", packfileInfo.RepositoryID)
			} else {
				fmt.Printf("Failed to query packfile for repo %d: %v, will attempt update\n", packfileInfo.RepositoryID, err)
			}
		} else {
			// Compare CIDs
			if chainPackfile.Cid == packfileInfo.CID {
				fmt.Printf("Packfile for repository %d already up-to-date (CID: %s), skipping\n", packfileInfo.RepositoryID, packfileInfo.CID)
				skipped++
				continue
			}
			fmt.Printf("Packfile for repository %d needs update (chain CID: %s -> new CID: %s)\n",
				packfileInfo.RepositoryID, chainPackfile.Cid, packfileInfo.CID)
		}

		// Create update message
		oldCid := ""
		if err == nil {
			oldCid = chainPackfile.Cid
		}

		msg := &storagetypes.MsgUpdateRepositoryPackfile{
			Creator:      batchMgr.client.Address().String(),
			RepositoryId: packfileInfo.RepositoryID,
			Name:         packfileInfo.Name,
			Cid:          packfileInfo.CID,
			RootHash:     packfileInfo.RootHash,
			Size_:        uint64(packfileInfo.Size),
			OldCid:       oldCid,
		}

		if err := batchMgr.AddToBatch(ctx, msg); err != nil {
			fmt.Printf("Failed to add packfile update for repo %d: %v\n", packfileInfo.RepositoryID, err)
			continue
		}

		fmt.Printf("Added packfile update for repository %d to batch\n", packfileInfo.RepositoryID)
		processed++
	}

	fmt.Printf("Packfile processing complete: %d processed, %d skipped\n", processed, skipped)
	return nil
}

// processReleaseAssets processes release assets using stored data
func processReleaseAssets(ctx context.Context, batchMgr *BatchTxManager, storageManager *shared.StorageManager) error {
	allReleaseAssets := storageManager.GetAllReleaseAssets()

	if allReleaseAssets == nil {
		fmt.Println("No release asset data retrieved (could be empty or error)")
		return nil
	}

	fmt.Printf("Found %d release assets to process\n", len(allReleaseAssets))

	// Group assets by repository and tag
	assetsByRelease := make(map[string][]*shared.ReleaseAssetInfo)
	for _, assetInfo := range allReleaseAssets {
		releaseKey := fmt.Sprintf("%d-%s", assetInfo.RepositoryID, assetInfo.TagName)
		assetsByRelease[releaseKey] = append(assetsByRelease[releaseKey], assetInfo)
	}

	for releaseKey, assets := range assetsByRelease {
		if len(assets) == 0 {
			continue
		}

		// Convert to storage types
		var assetUpdates []*storagetypes.ReleaseAssetUpdate
		for _, asset := range assets {
			assetUpdates = append(assetUpdates, &storagetypes.ReleaseAssetUpdate{
				Name:     asset.Name,
				Cid:      asset.CID,
				RootHash: asset.RootHash,
				Size_:    uint64(asset.Size),
				Sha256:   asset.SHA256,
				OldCid:   "", // Empty for initial update
			})
		}

		msg := &storagetypes.MsgUpdateReleaseAssets{
			Creator:      batchMgr.client.Address().String(),
			RepositoryId: assets[0].RepositoryID,
			Tag:          assets[0].TagName,
			Assets:       assetUpdates,
		}

		if err := batchMgr.AddToBatch(ctx, msg); err != nil {
			fmt.Printf("Failed to add release assets update for %s: %v\n", releaseKey, err)
			continue
		}

		fmt.Printf("Added release assets update for %s to batch (%d assets)\n", releaseKey, len(assets))
	}

	return nil
}

// processLFSObjects processes LFS objects using stored data
func processLFSObjects(ctx context.Context, batchMgr *BatchTxManager, storageManager *shared.StorageManager) error {
	allLFSObjects := storageManager.GetAllLFSObjects()

	if allLFSObjects == nil {
		fmt.Println("No LFS object data retrieved (could be empty or error)")
		return nil
	}

	fmt.Printf("Found %d LFS objects to process\n", len(allLFSObjects))

	for _, lfsInfo := range allLFSObjects {
		msg := &storagetypes.MsgUpdateLFSObject{
			Creator:      batchMgr.client.Address().String(),
			RepositoryId: lfsInfo.RepositoryID,
			Oid:          lfsInfo.OID,
			Cid:          lfsInfo.CID,
			RootHash:     lfsInfo.RootHash,
			Size_:        uint64(lfsInfo.Size),
		}

		if err := batchMgr.AddToBatch(ctx, msg); err != nil {
			fmt.Printf("Failed to add LFS object update for %s: %v\n", lfsInfo.OID, err)
			continue
		}

		fmt.Printf("Added LFS object update for %s to batch\n", lfsInfo.OID)
	}

	return nil
}

func main() {
	rootCmd := &cobra.Command{
		Use:               "update-script",
		Short:             "Update Gitopia storage with migration data",
		CompletionOptions: cobra.CompletionOptions{DisableDefaultCmd: true},
		PersistentPreRunE: func(cmd *cobra.Command, _ []string) error {
			return gc.CommandInit(cmd, AppName)
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := cmd.Context()

			// Get flags
			packfilesOnly, _ := cmd.Flags().GetBool("packfiles-only")

			// Initialize storage manager
			workingDir := viper.GetString("WORKING_DIR")
			fmt.Printf("Using working directory: %s\n", workingDir)

			storageManager := shared.NewStorageManager(workingDir)
			if err := storageManager.Load(); err != nil {
				return errors.Wrap(err, "failed to load storage manager")
			}
			fmt.Println("Storage manager loaded successfully")

			// Initialize Gitopia client
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

			// Initialize batch transaction manager
			batchMgr := NewBatchTxManager(gitopiaClient, BATCH_SIZE)

			if packfilesOnly {
				fmt.Printf("Starting packfiles-only update process with batch size: %d\n", BATCH_SIZE)
			} else {
				fmt.Printf("Starting batch update process with batch size: %d\n", BATCH_SIZE)
			}

			// Process repository packfiles
			fmt.Println("Processing repository packfiles...")
			if err := processRepositoryPackfiles(ctx, batchMgr, storageManager); err != nil {
				return errors.Wrap(err, "failed to process repository packfiles")
			}

			if !packfilesOnly {
				// Process release assets
				fmt.Println("Processing release assets...")
				if err := processReleaseAssets(ctx, batchMgr, storageManager); err != nil {
					return errors.Wrap(err, "failed to process release assets")
				}

				// Process LFS objects
				fmt.Println("Processing LFS objects...")
				if err := processLFSObjects(ctx, batchMgr, storageManager); err != nil {
					return errors.Wrap(err, "failed to process LFS objects")
				}
			} else {
				fmt.Println("Skipping release assets and LFS objects (packfiles-only mode)")
			}

			// Flush any remaining messages in the batch
			if err := batchMgr.FlushBatch(ctx); err != nil {
				return errors.Wrap(err, "failed to flush final batch")
			}

			fmt.Println("Update script completed successfully!")

			return nil
		},
	}

	// Add flags
	rootCmd.Flags().String("from", "", "Name or address of private key with which to sign")
	rootCmd.Flags().String("keyring-backend", "", "Select keyring's backend (os|file|kwallet|pass|test|memory)")
	rootCmd.Flags().Bool("packfiles-only", false, "Process only repository packfiles, skip release assets and LFS objects")

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
