package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/buger/jsonparser"
	"github.com/cosmos/cosmos-sdk/client"
	"github.com/cosmos/cosmos-sdk/client/keys"
	"github.com/cosmos/cosmos-sdk/client/tx"
	sdk "github.com/cosmos/cosmos-sdk/types"
	"github.com/gitopia/gitopia-go"
	"github.com/gitopia/gitopia-go/logger"
	"github.com/gitopia/gitopia-migration-tools/sync-daemon/handler"
	gitopiatypes "github.com/gitopia/gitopia/v5/x/gitopia/types"
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
	MsgMultiSetBranchQuery    = "tm.event='Tx' AND message.action='MsgMultiSetBranch'"
	MsgMultiSetTagQuery       = "tm.event='Tx' AND message.action='MsgMultiSetTag'"
	CreateReleaseQuery        = "tm.event='Tx' AND message.action='CreateRelease'"
	UpdateReleaseQuery        = "tm.event='Tx' AND message.action='UpdateRelease'"
	DeleteReleaseQuery        = "tm.event='Tx' AND message.action='DeleteRelease'"
	DaoCreateReleaseQuery     = "tm.event='Tx' AND message.action='DaoCreateRelease'"
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

	// Create WebSocket clients for event subscriptions
	client1, err := gitopia.NewWSEvents(ctx)
	if err != nil {
		return errors.Wrap(err, "failed to create WebSocket client 1")
	}
	defer client1.Close()

	client1Queries := []string{
		MsgMultiSetBranchQuery,
		MsgMultiSetTagQuery,
	}

	if err := client1.SubscribeQueries(ctx, client1Queries...); err != nil {
		return errors.Wrap(err, "failed to subscribe to client 1 events")
	}

	client1EventHandlers := map[string]func(context.Context, []byte) error{
		MsgMultiSetBranchQuery: branchHandler.Handle,
		MsgMultiSetTagQuery:    tagHandler.Handle,
	}

	// Client 2: Release events
	client2, err := gitopia.NewWSEvents(ctx)
	if err != nil {
		return errors.Wrap(err, "failed to create WebSocket client 2")
	}
	defer client2.Close()

	client2Queries := []string{
		CreateReleaseQuery,
		UpdateReleaseQuery,
		DeleteReleaseQuery,
		DaoCreateReleaseQuery,
	}

	if err := client2.SubscribeQueries(ctx, client2Queries...); err != nil {
		return errors.Wrap(err, "failed to subscribe to client 2 events")
	}

	client2EventHandlers := map[string]func(context.Context, []byte) error{
		CreateReleaseQuery: func(ctx context.Context, eventBuf []byte) error {
			return releaseHandler.Handle(ctx, eventBuf, "CreateRelease")
		},
		UpdateReleaseQuery: func(ctx context.Context, eventBuf []byte) error {
			return releaseHandler.Handle(ctx, eventBuf, "UpdateRelease")
		},
		DeleteReleaseQuery: func(ctx context.Context, eventBuf []byte) error {
			return releaseHandler.Handle(ctx, eventBuf, "DeleteRelease")
		},
		DaoCreateReleaseQuery: func(ctx context.Context, eventBuf []byte) error {
			return releaseHandler.Handle(ctx, eventBuf, "DaoCreateRelease")
		},
	}

	g, gCtx := errgroup.WithContext(ctx)

	// Start client 1 event processor
	g.Go(func() error {
		done, errChan := client1.ProcessEvents(gCtx, func(ctx context.Context, eventBuf []byte) error {
			return routeEventToHandler(ctx, eventBuf, client1EventHandlers)
		})
		select {
		case err := <-errChan:
			return errors.Wrap(err, "client 1 event processing error")
		case <-done:
			logger.FromContext(ctx).Info("client 1 event processing completed")
			return nil
		}
	})

	// Start client 2 event processor
	g.Go(func() error {
		done, errChan := client2.ProcessEvents(gCtx, func(ctx context.Context, eventBuf []byte) error {
			return routeEventToHandler(ctx, eventBuf, client2EventHandlers)
		})
		select {
		case err := <-errChan:
			return errors.Wrap(err, "client 2 event processing error")
		case <-done:
			logger.FromContext(ctx).Info("client 2 event processing completed")
			return nil
		}
	})

	return g.Wait()
}

// routeEventToHandler routes events to the appropriate handler based on the query field in the event.
func routeEventToHandler(ctx context.Context, eventBuf []byte, handlers map[string]func(context.Context, []byte) error) error {
	// Extract the query from the event buffer
	query, err := jsonparser.GetString(eventBuf, "query")
	if err != nil {
		logger.FromContext(ctx).WithField("event", string(eventBuf)).WithError(err).Debug("failed to parse query from event, ignoring")
		return nil
	}

	// Find the handler for this query
	if handler, ok := handlers[query]; ok {
		logger.FromContext(ctx).WithFields(logrus.Fields{"query": query}).Debug("routing event to handler")
		return handler(ctx, eventBuf)
	}

	// If no handler matches, log and continue
	logger.FromContext(ctx).WithField("query", query).Debug("no handler found for event query")
	return nil
}
