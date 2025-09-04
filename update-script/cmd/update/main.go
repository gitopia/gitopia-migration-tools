package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"

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
	ProgressFile         = "update_progress.json"
	BATCH_SIZE           = 100 // Process 100 messages per block
)

type UpdateProgress struct {
	ProcessedRepos      map[uint64]bool   `json:"processed_repos"`
	ProcessedReleases   map[string]bool   `json:"processed_releases"`
	ProcessedLFSObjects map[string]bool   `json:"processed_lfs_objects"`
	FailedRepos         map[uint64]string `json:"failed_repos"`
	FailedReleases      map[string]string `json:"failed_releases"`
	FailedLFSObjects    map[string]string `json:"failed_lfs_objects"`
	TotalProcessed      uint64            `json:"total_processed"`
}

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
	_, err := btm.client.BroadcastTx(ctx, btm.txQueue...)
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

// loadProgress loads progress from file
func loadProgress() (*UpdateProgress, error) {
	if _, err := os.Stat(ProgressFile); os.IsNotExist(err) {
		return &UpdateProgress{
			ProcessedRepos:      make(map[uint64]bool),
			ProcessedReleases:   make(map[string]bool),
			ProcessedLFSObjects: make(map[string]bool),
			FailedRepos:         make(map[uint64]string),
			FailedReleases:      make(map[string]string),
			FailedLFSObjects:    make(map[string]string),
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

	// Initialize maps if nil
	if progress.ProcessedRepos == nil {
		progress.ProcessedRepos = make(map[uint64]bool)
	}
	if progress.ProcessedReleases == nil {
		progress.ProcessedReleases = make(map[string]bool)
	}
	if progress.ProcessedLFSObjects == nil {
		progress.ProcessedLFSObjects = make(map[string]bool)
	}
	if progress.FailedRepos == nil {
		progress.FailedRepos = make(map[uint64]string)
	}
	if progress.FailedReleases == nil {
		progress.FailedReleases = make(map[string]string)
	}
	if progress.FailedLFSObjects == nil {
		progress.FailedLFSObjects = make(map[string]string)
	}

	return &progress, nil
}

// saveProgress saves progress to file
func saveProgress(progress *UpdateProgress) error {
	data, err := json.MarshalIndent(progress, "", "  ")
	if err != nil {
		return errors.Wrap(err, "failed to marshal progress")
	}
	return os.WriteFile(ProgressFile, data, 0644)
}

// processRepositoryPackfiles processes repository packfiles using stored data
func processRepositoryPackfiles(ctx context.Context, batchMgr *BatchTxManager, storageManager *shared.StorageManager, progress *UpdateProgress) error {
	allPackfiles := storageManager.GetAllPackfileInfo()
	
	for _, packfileInfo := range allPackfiles {
		if progress.ProcessedRepos[packfileInfo.RepositoryID] {
			continue // Skip already processed
		}
		
		msg := &storagetypes.MsgUpdateRepositoryPackfile{
			Creator:      batchMgr.client.ClientAddress(),
			RepositoryId: packfileInfo.RepositoryID,
			Name:         packfileInfo.Name,
			Cid:          packfileInfo.CID,
			RootHash:     packfileInfo.RootHash,
			Size_:        uint64(packfileInfo.Size),
			OldCid:       "", // Empty for initial update
		}
		
		if err := batchMgr.AddToBatch(ctx, msg); err != nil {
			progress.FailedRepos[packfileInfo.RepositoryID] = err.Error()
			fmt.Printf("Failed to add packfile update for repo %d: %v\n", packfileInfo.RepositoryID, err)
			continue
		}
		
		progress.ProcessedRepos[packfileInfo.RepositoryID] = true
		progress.TotalProcessed++
		fmt.Printf("Added packfile update for repository %d to batch\n", packfileInfo.RepositoryID)
	}
	
	return nil
}

// processReleaseAssets processes release assets using stored data
func processReleaseAssets(ctx context.Context, batchMgr *BatchTxManager, storageManager *shared.StorageManager, progress *UpdateProgress) error {
	allReleaseAssets := storageManager.GetAllReleaseAssetInfo()
	
	// Group assets by repository and tag
	assetsByRelease := make(map[string][]*shared.ReleaseAssetInfo)
	for _, assetInfo := range allReleaseAssets {
		releaseKey := fmt.Sprintf("%d-%s", assetInfo.RepositoryID, assetInfo.TagName)
		if progress.ProcessedReleases[releaseKey] {
			continue // Skip already processed
		}
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
			Creator:      batchMgr.client.ClientAddress(),
			RepositoryId: assets[0].RepositoryID,
			TagName:      assets[0].TagName,
			Assets:       assetUpdates,
		}
		
		if err := batchMgr.AddToBatch(ctx, msg); err != nil {
			progress.FailedReleases[releaseKey] = err.Error()
			fmt.Printf("Failed to add release assets update for %s: %v\n", releaseKey, err)
			continue
		}
		
		progress.ProcessedReleases[releaseKey] = true
		progress.TotalProcessed++
		fmt.Printf("Added release assets update for %s to batch (%d assets)\n", releaseKey, len(assets))
	}
	
	return nil
}

// processLFSObjects processes LFS objects using stored data
func processLFSObjects(ctx context.Context, batchMgr *BatchTxManager, storageManager *shared.StorageManager, progress *UpdateProgress) error {
	allLFSObjects := storageManager.GetAllLFSObjectInfo()
	
	for _, lfsInfo := range allLFSObjects {
		lfsKey := fmt.Sprintf("%d-%s", lfsInfo.RepositoryID, lfsInfo.OID)
		if progress.ProcessedLFSObjects[lfsKey] {
			continue // Skip already processed
		}
		
		msg := &storagetypes.MsgUpdateLFSObject{
			Creator:      batchMgr.client.ClientAddress(),
			RepositoryId: lfsInfo.RepositoryID,
			Oid:          lfsInfo.OID,
			Cid:          lfsInfo.CID,
			RootHash:     lfsInfo.RootHash,
			Size_:        uint64(lfsInfo.Size),
		}
		
		if err := batchMgr.AddToBatch(ctx, msg); err != nil {
			progress.FailedLFSObjects[lfsKey] = err.Error()
			fmt.Printf("Failed to add LFS object update for %s: %v\n", lfsKey, err)
			continue
		}
		
		progress.ProcessedLFSObjects[lfsKey] = true
		progress.TotalProcessed++
		fmt.Printf("Added LFS object update for %s to batch\n", lfsKey)
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
			
			fmt.Printf("Starting batch update process with batch size: %d\n", BATCH_SIZE)
			
			// Process repository packfiles
			fmt.Println("Processing repository packfiles...")
			if err := processRepositoryPackfiles(ctx, batchMgr, storageManager, progress); err != nil {
				return errors.Wrap(err, "failed to process repository packfiles")
			}
			
			// Process release assets
			fmt.Println("Processing release assets...")
			if err := processReleaseAssets(ctx, batchMgr, storageManager, progress); err != nil {
				return errors.Wrap(err, "failed to process release assets")
			}
			
			// Process LFS objects
			fmt.Println("Processing LFS objects...")
			if err := processLFSObjects(ctx, batchMgr, storageManager, progress); err != nil {
				return errors.Wrap(err, "failed to process LFS objects")
			}
			
			// Flush any remaining messages in the batch
			if err := batchMgr.FlushBatch(ctx); err != nil {
				return errors.Wrap(err, "failed to flush final batch")
			}
			
			// Save final progress
			if err := saveProgress(progress); err != nil {
				return errors.Wrap(err, "failed to save final progress")
			}
			
			fmt.Printf("Update script completed successfully! Total processed: %d\n", progress.TotalProcessed)
			fmt.Printf("Failed repos: %d, Failed releases: %d, Failed LFS objects: %d\n", 
				len(progress.FailedRepos), len(progress.FailedReleases), len(progress.FailedLFSObjects))
			
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
