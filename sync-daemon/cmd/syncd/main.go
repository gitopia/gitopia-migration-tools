package main

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"time"

	"github.com/cosmos/cosmos-sdk/client"
	"github.com/cosmos/cosmos-sdk/client/keys"
	"github.com/cosmos/cosmos-sdk/client/tx"
	sdk "github.com/cosmos/cosmos-sdk/types"
	"github.com/gitopia/gitopia-go"
	"github.com/gitopia/gitopia-go/logger"
	"github.com/gitopia/gitopia-migration-tools/sync-daemon/handler"
	ipfsclusterclient "github.com/ipfs-cluster/ipfs-cluster/api/rest/client"
	"github.com/ipfs/kubo/client/rpc"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"golang.org/x/sync/errgroup"
)

const (
	AccountAddressPrefix = "gitopia"
	AccountPubKeyPrefix  = AccountAddressPrefix + sdk.PrefixPublic
	AppName              = "sync-daemon"

	// Event queries for subscription
	MsgMultiSetBranchQuery   = "tm.event='Tx' AND message.action='MsgMultiSetBranch'"
	MsgMultiSetTagQuery      = "tm.event='Tx' AND message.action='MsgMultiSetTag'"
	CreateReleaseQuery       = "tm.event='Tx' AND message.action='CreateRelease'"
	UpdateReleaseQuery       = "tm.event='Tx' AND message.action='UpdateRelease'"
	DeleteReleaseQuery       = "tm.event='Tx' AND message.action='DeleteRelease'"
	DaoCreateReleaseQuery    = "tm.event='Tx' AND message.action='DaoCreateRelease'"
	SetPullRequestStateQuery = "tm.event='Tx' AND message.action='SetPullRequestState'"
)

func main() {
	rootCmd := &cobra.Command{
		Use:               "sync-daemon",
		Short:             "Sync git-server updates to storage provider",
		CompletionOptions: cobra.CompletionOptions{DisableDefaultCmd: true},
		PersistentPreRunE: func(cmd *cobra.Command, _ []string) error {
			return gitopia.CommandInit(cmd, AppName)
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			return startSyncDaemon(cmd)
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

	gitopia.WithAppName(AppName)
	gitopia.WithChainId(viper.GetString("CHAIN_ID"))
	gitopia.WithGasPrices(viper.GetString("GAS_PRICES"))
	gitopia.WithGitopiaAddr(viper.GetString("GITOPIA_ADDR"))
	gitopia.WithTmAddr(viper.GetString("TM_ADDR"))
	gitopia.WithWorkingDir(viper.GetString("WORKING_DIR"))

	rootCmd.AddCommand(keys.Commands(viper.GetString("WORKING_DIR")))

	if err := rootCmd.ExecuteContext(ctx); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

func startSyncDaemon(cmd *cobra.Command) error {
	g, ctx := errgroup.WithContext(cmd.Context())

	// Initialize Gitopia client
	clientCtx := client.GetClientContextFromCmd(cmd)
	txf, err := tx.NewFactoryCLI(clientCtx, cmd.Flags())
	if err != nil {
		return errors.Wrap(err, "error initializing tx factory")
	}

	gitopiaClient, err := gitopia.NewClient(ctx, clientCtx, txf)
	if err != nil {
		return errors.WithMessage(err, "gitopia client error")
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

	// Initialize IPFS HTTP API client
	ipfsHttpApi, err := rpc.NewURLApiWithClient(
		fmt.Sprintf("http://%s:%s", viper.GetString("IPFS_HOST"), viper.GetString("IPFS_PORT")),
		&http.Client{},
	)
	if err != nil {
		return errors.Wrap(err, "failed to create IPFS API")
	}

	// Start the event processor to handle blockchain events
	g.Go(func() error {
		return startEventProcessor(ctx, gitopiaClient, ipfsClusterClient, ipfsHttpApi)
	})

	logrus.Info("sync daemon started")
	return g.Wait()
}

func startEventProcessor(ctx context.Context, gitopiaClient gitopia.Client, ipfsClusterClient ipfsclusterclient.Client, ipfsHttpApi *rpc.HttpApi) error {
	logger.FromContext(ctx).Info("starting event processor for sync daemon")

	// Initialize handlers
	branchHandler := handler.NewBranchEventHandler(gitopiaClient, ipfsClusterClient, ipfsHttpApi)
	tagHandler := handler.NewTagEventHandler(gitopiaClient, ipfsClusterClient, ipfsHttpApi)
	releaseHandler := handler.NewReleaseEventHandler(gitopiaClient, ipfsClusterClient)
	pullRequestHandler := handler.NewPullRequestEventHandler(gitopiaClient, ipfsClusterClient, ipfsHttpApi)

	// Create separate WebSocket clients for each query (older gitopia-go doesn't support multiple queries)
	branchClient, err := gitopia.NewWSEvents(ctx, MsgMultiSetBranchQuery)
	if err != nil {
		return errors.WithMessage(err, "branch client error")
	}

	tagClient, err := gitopia.NewWSEvents(ctx, MsgMultiSetTagQuery)
	if err != nil {
		return errors.WithMessage(err, "tag client error")
	}

	createReleaseClient, err := gitopia.NewWSEvents(ctx, CreateReleaseQuery)
	if err != nil {
		return errors.WithMessage(err, "create release client error")
	}

	updateReleaseClient, err := gitopia.NewWSEvents(ctx, UpdateReleaseQuery)
	if err != nil {
		return errors.WithMessage(err, "update release client error")
	}

	deleteReleaseClient, err := gitopia.NewWSEvents(ctx, DeleteReleaseQuery)
	if err != nil {
		return errors.WithMessage(err, "delete release client error")
	}

	daoCreateReleaseClient, err := gitopia.NewWSEvents(ctx, DaoCreateReleaseQuery)
	if err != nil {
		return errors.WithMessage(err, "dao create release client error")
	}

	setPullRequestStateClient, err := gitopia.NewWSEvents(ctx, SetPullRequestStateQuery)
	if err != nil {
		return errors.WithMessage(err, "set pull request state client error")
	}

	g, gCtx := errgroup.WithContext(ctx)

	// Subscribe to branch events
	g.Go(func() error {
		done, errChan := branchClient.Subscribe(gCtx, branchHandler.Handle)
		select {
		case err := <-errChan:
			return errors.WithMessage(err, "branch subscribe error")
		case <-done:
			logger.FromContext(ctx).Info("branch done")
			return nil
		}
	})

	// Subscribe to tag events
	g.Go(func() error {
		done, errChan := tagClient.Subscribe(gCtx, tagHandler.Handle)
		select {
		case err := <-errChan:
			return errors.WithMessage(err, "tag subscribe error")
		case <-done:
			logger.FromContext(ctx).Info("tag done")
			return nil
		}
	})

	// Subscribe to create release events
	g.Go(func() error {
		done, errChan := createReleaseClient.Subscribe(gCtx, func(ctx context.Context, eventBuf []byte) error {
			return releaseHandler.Handle(ctx, eventBuf, "CreateRelease")
		})
		select {
		case err := <-errChan:
			return errors.WithMessage(err, "create release subscribe error")
		case <-done:
			logger.FromContext(ctx).Info("create release done")
			return nil
		}
	})

	// Subscribe to update release events
	g.Go(func() error {
		done, errChan := updateReleaseClient.Subscribe(gCtx, func(ctx context.Context, eventBuf []byte) error {
			return releaseHandler.Handle(ctx, eventBuf, "UpdateRelease")
		})
		select {
		case err := <-errChan:
			return errors.WithMessage(err, "update release subscribe error")
		case <-done:
			logger.FromContext(ctx).Info("update release done")
			return nil
		}
	})

	// Subscribe to delete release events
	g.Go(func() error {
		done, errChan := deleteReleaseClient.Subscribe(gCtx, func(ctx context.Context, eventBuf []byte) error {
			return releaseHandler.Handle(ctx, eventBuf, "DeleteRelease")
		})
		select {
		case err := <-errChan:
			return errors.WithMessage(err, "delete release subscribe error")
		case <-done:
			logger.FromContext(ctx).Info("delete release done")
			return nil
		}
	})

	// Subscribe to DAO create release events
	g.Go(func() error {
		done, errChan := daoCreateReleaseClient.Subscribe(gCtx, func(ctx context.Context, eventBuf []byte) error {
			return releaseHandler.Handle(ctx, eventBuf, "DaoCreateRelease")
		})
		select {
		case err := <-errChan:
			return errors.WithMessage(err, "dao create release subscribe error")
		case <-done:
			logger.FromContext(ctx).Info("dao create release done")
			return nil
		}
	})

	// Subscribe to set pull request state events
	g.Go(func() error {
		done, errChan := setPullRequestStateClient.Subscribe(gCtx, pullRequestHandler.Handle)
		select {
		case err := <-errChan:
			return errors.WithMessage(err, "set pull request state subscribe error")
		case <-done:
			logger.FromContext(ctx).Info("set pull request state done")
			return nil
		}
	})

	return g.Wait()
}
