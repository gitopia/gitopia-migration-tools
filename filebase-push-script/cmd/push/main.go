package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/cosmos/cosmos-sdk/client"
	"github.com/cosmos/cosmos-sdk/client/keys"
	"github.com/cosmos/cosmos-sdk/client/tx"
	sdk "github.com/cosmos/cosmos-sdk/types"
	"github.com/cosmos/cosmos-sdk/types/query"
	gc "github.com/gitopia/gitopia-go"
	"github.com/gitopia/gitopia-go/logger"
	storagetypes "github.com/gitopia/gitopia/v6/x/storage/types"
	"github.com/pkg/errors"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

const (
	AccountAddressPrefix = "gitopia"
	AccountPubKeyPrefix  = AccountAddressPrefix + sdk.PrefixPublic
	AppName              = "filebase-push-script"
	PageLimit            = 100 // Number of items per page for queries
	TempDir              = "/tmp/filebase-push"
)

// ProgressTracker tracks uploaded files to avoid duplicates and support resume
type ProgressTracker struct {
	UploadedFiles map[string]UploadedFile `json:"uploaded_files"`
	filePath      string
}

type UploadedFile struct {
	CID       string    `json:"cid"`
	Name      string    `json:"name"`
	Type      string    `json:"type"` // "packfile", "release_asset", "lfs_object"
	Size      int64     `json:"size"`
	Timestamp time.Time `json:"timestamp"`
}

// FilebaseClient wraps S3 client for Filebase operations
type FilebaseClient struct {
	s3Client *s3.S3
	bucket   string
}

// IPFSClient handles downloading from local IPFS server
type IPFSClient struct {
	host string
	port string
}

func NewProgressTracker(filePath string) (*ProgressTracker, error) {
	pt := &ProgressTracker{
		UploadedFiles: make(map[string]UploadedFile),
		filePath:      filePath,
	}

	// Load existing progress if file exists
	if _, err := os.Stat(filePath); err == nil {
		data, err := os.ReadFile(filePath)
		if err != nil {
			return nil, errors.Wrap(err, "failed to read progress file")
		}
		if err := json.Unmarshal(data, pt); err != nil {
			return nil, errors.Wrap(err, "failed to unmarshal progress file")
		}
	}

	return pt, nil
}

func (pt *ProgressTracker) IsUploaded(key string) bool {
	_, exists := pt.UploadedFiles[key]
	return exists
}

func (pt *ProgressTracker) MarkUploaded(key string, file UploadedFile) error {
	pt.UploadedFiles[key] = file
	return pt.Save()
}

func (pt *ProgressTracker) Save() error {
	data, err := json.MarshalIndent(pt, "", "  ")
	if err != nil {
		return errors.Wrap(err, "failed to marshal progress tracker")
	}
	return os.WriteFile(pt.filePath, data, 0644)
}

func NewFilebaseClient(accessKey, secretKey, bucket, region, endpoint string) (*FilebaseClient, error) {
	s3Config := &aws.Config{
		Credentials:      credentials.NewStaticCredentials(accessKey, secretKey, ""),
		Endpoint:         aws.String(endpoint),
		Region:           aws.String(region),
		S3ForcePathStyle: aws.Bool(true),
	}

	sess, err := session.NewSessionWithOptions(session.Options{
		Config: *s3Config,
	})
	if err != nil {
		return nil, errors.Wrap(err, "failed to create AWS session")
	}

	return &FilebaseClient{
		s3Client: s3.New(sess),
		bucket:   bucket,
	}, nil
}

func (fc *FilebaseClient) UploadFile(ctx context.Context, filePath, key string) error {
	file, err := os.Open(filePath)
	if err != nil {
		return errors.Wrap(err, "failed to open file")
	}
	defer file.Close()

	putObjectInput := &s3.PutObjectInput{
		Body:   file,
		Bucket: aws.String(fc.bucket),
		Key:    aws.String(key),
	}

	_, err = fc.s3Client.PutObjectWithContext(ctx, putObjectInput)
	if err != nil {
		return errors.Wrap(err, "failed to upload file to Filebase")
	}

	return nil
}

func NewIPFSClient(host, port string) *IPFSClient {
	return &IPFSClient{
		host: host,
		port: port,
	}
}

func (ic *IPFSClient) DownloadFile(ctx context.Context, cid, outputPath string) error {
	url := fmt.Sprintf("http://%s:%s/api/v0/cat?arg=%s", ic.host, ic.port, cid)

	req, err := http.NewRequestWithContext(ctx, "POST", url, nil)
	if err != nil {
		return errors.Wrap(err, "failed to create request")
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return errors.Wrap(err, "failed to download from IPFS")
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("IPFS download failed with status: %d", resp.StatusCode)
	}

	// Create output directory if it doesn't exist
	if err := os.MkdirAll(filepath.Dir(outputPath), 0755); err != nil {
		return errors.Wrap(err, "failed to create output directory")
	}

	outFile, err := os.Create(outputPath)
	if err != nil {
		return errors.Wrap(err, "failed to create output file")
	}
	defer outFile.Close()

	_, err = io.Copy(outFile, resp.Body)
	if err != nil {
		return errors.Wrap(err, "failed to write file")
	}

	return nil
}

// Query functions for fetching data from Gitopia chain
func queryAllPackfiles(ctx context.Context, client gc.Client) ([]storagetypes.Packfile, error) {
	var allPackfiles []storagetypes.Packfile
	var nextKey []byte

	for {
		req := &storagetypes.QueryPackfilesRequest{
			Pagination: &query.PageRequest{
				Key:   nextKey,
				Limit: PageLimit,
			},
		}

		resp, err := client.QueryClient().Storage.Packfiles(ctx, req)
		if err != nil {
			return nil, errors.Wrap(err, "failed to query packfiles")
		}

		allPackfiles = append(allPackfiles, resp.Packfiles...)

		if resp.Pagination.NextKey == nil {
			break
		}
		nextKey = resp.Pagination.NextKey
	}

	return allPackfiles, nil
}

func queryAllReleaseAssets(ctx context.Context, client gc.Client) ([]storagetypes.ReleaseAsset, error) {
	var allAssets []storagetypes.ReleaseAsset
	var nextKey []byte

	for {
		req := &storagetypes.QueryReleaseAssetsRequest{
			Pagination: &query.PageRequest{
				Key:   nextKey,
				Limit: PageLimit,
			},
		}

		resp, err := client.QueryClient().Storage.ReleaseAssets(ctx, req)
		if err != nil {
			return nil, errors.Wrap(err, "failed to query release assets")
		}

		allAssets = append(allAssets, resp.ReleaseAssets...)

		if resp.Pagination.NextKey == nil {
			break
		}
		nextKey = resp.Pagination.NextKey
	}

	return allAssets, nil
}

func queryAllLFSObjects(ctx context.Context, client gc.Client) ([]storagetypes.LFSObject, error) {
	var allObjects []storagetypes.LFSObject
	var nextKey []byte

	for {
		req := &storagetypes.QueryLFSObjectsRequest{
			Pagination: &query.PageRequest{
				Key:   nextKey,
				Limit: PageLimit,
			},
		}

		resp, err := client.QueryClient().Storage.LFSObjects(ctx, req)
		if err != nil {
			return nil, errors.Wrap(err, "failed to query LFS objects")
		}

		allObjects = append(allObjects, resp.LfsObjects...)

		if resp.Pagination.NextKey == nil {
			break
		}
		nextKey = resp.Pagination.NextKey
	}

	return allObjects, nil
}

func processPackfiles(ctx context.Context, packfiles []storagetypes.Packfile, ipfsClient *IPFSClient, filebaseClient *FilebaseClient, tracker *ProgressTracker) error {
	fmt.Printf("Processing %d packfiles...\n", len(packfiles))

	for i, packfile := range packfiles {
		key := fmt.Sprintf("packfile-%s", packfile.Name)

		if tracker.IsUploaded(key) {
			fmt.Printf("[%d/%d] Packfile %s already uploaded, skipping\n", i+1, len(packfiles), packfile.Name)
			continue
		}

		fmt.Printf("[%d/%d] Processing packfile %s (CID: %s)\n", i+1, len(packfiles), packfile.Name, packfile.Cid)

		// Download from IPFS
		tempFile := filepath.Join(TempDir, fmt.Sprintf("packfile-%s", packfile.Name))
		if err := ipfsClient.DownloadFile(ctx, packfile.Cid, tempFile); err != nil {
			fmt.Printf("Failed to download packfile %s: %v\n", packfile.Name, err)
			continue
		}

		// Upload to Filebase
		filebaseKey := fmt.Sprintf("packfiles/%s", packfile.Name)
		if err := filebaseClient.UploadFile(ctx, tempFile, filebaseKey); err != nil {
			fmt.Printf("Failed to upload packfile %s to Filebase: %v\n", packfile.Name, err)
			os.Remove(tempFile)
			continue
		}

		// Mark as uploaded
		uploadedFile := UploadedFile{
			CID:       packfile.Cid,
			Name:      packfile.Name,
			Type:      "packfile",
			Size:      int64(packfile.Size_),
			Timestamp: time.Now(),
		}
		if err := tracker.MarkUploaded(key, uploadedFile); err != nil {
			fmt.Printf("Failed to update progress tracker: %v\n", err)
		}

		// Clean up temp file
		os.Remove(tempFile)
		fmt.Printf("[%d/%d] Successfully uploaded packfile %s\n", i+1, len(packfiles), packfile.Name)
	}

	return nil
}

func processReleaseAssets(ctx context.Context, assets []storagetypes.ReleaseAsset, ipfsClient *IPFSClient, filebaseClient *FilebaseClient, tracker *ProgressTracker) error {
	fmt.Printf("Processing %d release assets...\n", len(assets))

	for i, asset := range assets {
		key := fmt.Sprintf("asset-%s", asset.Sha256)

		if tracker.IsUploaded(key) {
			fmt.Printf("[%d/%d] Release asset %s already uploaded, skipping\n", i+1, len(assets), asset.Name)
			continue
		}

		fmt.Printf("[%d/%d] Processing release asset %s (CID: %s, SHA256: %s)\n", i+1, len(assets), asset.Name, asset.Cid, asset.Sha256)

		// Download from IPFS
		tempFile := filepath.Join(TempDir, fmt.Sprintf("asset-%s", asset.Sha256))
		if err := ipfsClient.DownloadFile(ctx, asset.Cid, tempFile); err != nil {
			fmt.Printf("Failed to download release asset %s: %v\n", asset.Name, err)
			continue
		}

		// Upload to Filebase
		filebaseKey := fmt.Sprintf("release-assets/%s", asset.Sha256)
		if err := filebaseClient.UploadFile(ctx, tempFile, filebaseKey); err != nil {
			fmt.Printf("Failed to upload release asset %s to Filebase: %v\n", asset.Name, err)
			os.Remove(tempFile)
			continue
		}

		// Mark as uploaded
		uploadedFile := UploadedFile{
			CID:       asset.Cid,
			Name:      asset.Name,
			Type:      "release_asset",
			Size:      int64(asset.Size_),
			Timestamp: time.Now(),
		}
		if err := tracker.MarkUploaded(key, uploadedFile); err != nil {
			fmt.Printf("Failed to update progress tracker: %v\n", err)
		}

		// Clean up temp file
		os.Remove(tempFile)
		fmt.Printf("[%d/%d] Successfully uploaded release asset %s\n", i+1, len(assets), asset.Name)
	}

	return nil
}

func processLFSObjects(ctx context.Context, objects []storagetypes.LFSObject, ipfsClient *IPFSClient, filebaseClient *FilebaseClient, tracker *ProgressTracker) error {
	fmt.Printf("Processing %d LFS objects...\n", len(objects))

	for i, obj := range objects {
		key := fmt.Sprintf("lfs-%s", obj.Oid)

		if tracker.IsUploaded(key) {
			fmt.Printf("[%d/%d] LFS object %s already uploaded, skipping\n", i+1, len(objects), obj.Oid)
			continue
		}

		fmt.Printf("[%d/%d] Processing LFS object %s (CID: %s)\n", i+1, len(objects), obj.Oid, obj.Cid)

		// Download from IPFS
		tempFile := filepath.Join(TempDir, fmt.Sprintf("lfs-%s", obj.Oid))
		if err := ipfsClient.DownloadFile(ctx, obj.Cid, tempFile); err != nil {
			fmt.Printf("Failed to download LFS object %s: %v\n", obj.Oid, err)
			continue
		}

		// Upload to Filebase
		filebaseKey := fmt.Sprintf("lfs-objects/%s", obj.Oid)
		if err := filebaseClient.UploadFile(ctx, tempFile, filebaseKey); err != nil {
			fmt.Printf("Failed to upload LFS object %s to Filebase: %v\n", obj.Oid, err)
			os.Remove(tempFile)
			continue
		}

		// Mark as uploaded
		uploadedFile := UploadedFile{
			CID:       obj.Cid,
			Name:      obj.Oid,
			Type:      "lfs_object",
			Size:      int64(obj.Size_),
			Timestamp: time.Now(),
		}
		if err := tracker.MarkUploaded(key, uploadedFile); err != nil {
			fmt.Printf("Failed to update progress tracker: %v\n", err)
		}

		// Clean up temp file
		os.Remove(tempFile)
		fmt.Printf("[%d/%d] Successfully uploaded LFS object %s\n", i+1, len(objects), obj.Oid)
	}

	return nil
}

func main() {
	rootCmd := &cobra.Command{
		Use:               "filebase-push",
		Short:             "Push packfiles, release assets, and LFS objects to Filebase",
		CompletionOptions: cobra.CompletionOptions{DisableDefaultCmd: true},
		PersistentPreRunE: func(cmd *cobra.Command, _ []string) error {
			return gc.CommandInit(cmd, AppName)
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := cmd.Context()

			// Get flags
			packfilesOnly, _ := cmd.Flags().GetBool("packfiles-only")
			assetsOnly, _ := cmd.Flags().GetBool("assets-only")
			lfsOnly, _ := cmd.Flags().GetBool("lfs-only")
			progressFile, _ := cmd.Flags().GetString("progress-file")

			// Validate configuration
			requiredVars := []string{
				"FILEBASE_ACCESS_KEY", "FILEBASE_SECRET_KEY", "FILEBASE_BUCKET",
				"FILEBASE_REGION", "FILEBASE_ENDPOINT", "IPFS_HOST", "IPFS_PORT",
			}
			for _, v := range requiredVars {
				if viper.GetString(v) == "" {
					return fmt.Errorf("required configuration variable %s is not set", v)
				}
			}

			// Create temp directory
			if err := os.MkdirAll(TempDir, 0755); err != nil {
				return errors.Wrap(err, "failed to create temp directory")
			}

			// Initialize progress tracker
			tracker, err := NewProgressTracker(progressFile)
			if err != nil {
				return errors.Wrap(err, "failed to initialize progress tracker")
			}

			// Initialize IPFS client
			ipfsClient := NewIPFSClient(viper.GetString("IPFS_HOST"), viper.GetString("IPFS_PORT"))

			// Initialize Filebase client
			filebaseClient, err := NewFilebaseClient(
				viper.GetString("FILEBASE_ACCESS_KEY"),
				viper.GetString("FILEBASE_SECRET_KEY"),
				viper.GetString("FILEBASE_BUCKET"),
				viper.GetString("FILEBASE_REGION"),
				viper.GetString("FILEBASE_ENDPOINT"),
			)
			if err != nil {
				return errors.Wrap(err, "failed to initialize Filebase client")
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

			fmt.Println("Starting Filebase push process...")

			// Process packfiles
			if !assetsOnly && !lfsOnly {
				fmt.Println("Querying packfiles from chain...")
				packfiles, err := queryAllPackfiles(ctx, gitopiaClient)
				if err != nil {
					return errors.Wrap(err, "failed to query packfiles")
				}

				if err := processPackfiles(ctx, packfiles, ipfsClient, filebaseClient, tracker); err != nil {
					return errors.Wrap(err, "failed to process packfiles")
				}
			}

			// Process release assets
			if !packfilesOnly && !lfsOnly {
				fmt.Println("Querying release assets from chain...")
				assets, err := queryAllReleaseAssets(ctx, gitopiaClient)
				if err != nil {
					return errors.Wrap(err, "failed to query release assets")
				}

				if err := processReleaseAssets(ctx, assets, ipfsClient, filebaseClient, tracker); err != nil {
					return errors.Wrap(err, "failed to process release assets")
				}
			}

			// Process LFS objects
			if !packfilesOnly && !assetsOnly {
				fmt.Println("Querying LFS objects from chain...")
				objects, err := queryAllLFSObjects(ctx, gitopiaClient)
				if err != nil {
					return errors.Wrap(err, "failed to query LFS objects")
				}

				if err := processLFSObjects(ctx, objects, ipfsClient, filebaseClient, tracker); err != nil {
					return errors.Wrap(err, "failed to process LFS objects")
				}
			}

			fmt.Println("Filebase push process completed successfully!")
			fmt.Printf("Progress saved to: %s\n", progressFile)

			return nil
		},
	}

	// Add flags
	rootCmd.Flags().Bool("packfiles-only", false, "Process only packfiles")
	rootCmd.Flags().Bool("assets-only", false, "Process only release assets")
	rootCmd.Flags().Bool("lfs-only", false, "Process only LFS objects")
	rootCmd.Flags().String("progress-file", "filebase-push-progress.json", "File to store upload progress")

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
