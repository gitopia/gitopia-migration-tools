package handler

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"

	gc "github.com/gitopia/gitopia-go"
	"github.com/gitopia/gitopia-go/logger"
	"github.com/gitopia/gitopia-migration-tools/utils"
	gitopiatypes "github.com/gitopia/gitopia/v5/x/gitopia/types"
	ipfsclusterclient "github.com/ipfs-cluster/ipfs-cluster/api/rest/client"
	"github.com/ipfs/kubo/client/rpc"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
)

type TagEventHandler struct {
	gitopiaClient     gc.Client
	ipfsClusterClient ipfsclusterclient.Client
	ipfsHttpApi       *rpc.HttpApi
}

func NewTagEventHandler(gitopiaClient gc.Client, ipfsClusterClient ipfsclusterclient.Client, ipfsHttpApi *rpc.HttpApi) *TagEventHandler {
	return &TagEventHandler{
		gitopiaClient:     gitopiaClient,
		ipfsClusterClient: ipfsClusterClient,
		ipfsHttpApi:       ipfsHttpApi,
	}
}

func (h *TagEventHandler) Handle(ctx context.Context, eventBuf []byte) error {
	// Extract repository IDs from the event
	repoIDs, err := ExtractStringArray(eventBuf, "message", gitopiatypes.EventAttributeRepoIdKey)
	if err != nil {
		return errors.Wrap(err, "error parsing repository id")
	}

	for _, repoIDStr := range repoIDs {
		repoID, err := strconv.ParseUint(repoIDStr, 10, 64)
		if err != nil {
			logger.FromContext(ctx).WithError(err).WithField("repo_id", repoIDStr).Error("failed to parse repository ID")
			continue
		}

		if err := h.processRepository(ctx, repoID); err != nil {
			logger.FromContext(ctx).WithError(err).WithField("repository_id", repoID).Error("failed to process repository tag update")
		}
	}

	return nil
}

func (h *TagEventHandler) processRepository(ctx context.Context, repositoryID uint64) error {
	logger.FromContext(ctx).WithField("repository_id", repositoryID).Info("processing repository tag update")

	// Get repository info
	repository, err := h.gitopiaClient.QueryClient().Gitopia.Repository(ctx, &gitopiatypes.QueryGetRepositoryRequest{
		Id: repositoryID,
	})
	if err != nil {
		return errors.Wrap(err, "failed to get repository")
	}

	gitDir := viper.GetString("GIT_REPOS_DIR")
	repoDir := filepath.Join(gitDir, fmt.Sprintf("%d.git", repositoryID))

	// Check if repository directory exists, if not clone it
	if _, err := os.Stat(repoDir); os.IsNotExist(err) {
		logger.FromContext(ctx).WithField("repository_id", repositoryID).Info("repository not found locally, cloning")
		
		remoteUrl := fmt.Sprintf("gitopia://%s/%s", repository.Repository.Owner.Id, repository.Repository.Name)
		cmd := exec.Command("git", "clone", "--bare", remoteUrl, repoDir)
		if err := cmd.Run(); err != nil {
			return errors.Wrapf(err, "error cloning repository %d", repositoryID)
		}
	} else {
		// Update existing repository
		logger.FromContext(ctx).WithField("repository_id", repositoryID).Info("updating existing repository tags")
		
		cmd := exec.Command("git", "fetch", "--tags", "--force")
		cmd.Dir = repoDir
		if err := cmd.Run(); err != nil {
			return errors.Wrapf(err, "error fetching tag updates for repository %d", repositoryID)
		}
	}

	// Run git gc to optimize and create new packfiles
	cmd := exec.Command("git", "gc")
	cmd.Dir = repoDir
	if err := cmd.Run(); err != nil {
		return errors.Wrapf(err, "error running git gc for repo %d", repositoryID)
	}

	// Pin new packfiles to IPFS cluster
	packDir := filepath.Join(repoDir, "objects", "pack")
	packFiles, err := filepath.Glob(filepath.Join(packDir, "*.pack"))
	if err != nil {
		return errors.Wrapf(err, "error finding packfiles for repo %d", repositoryID)
	}

	for _, packfileName := range packFiles {
		cid, err := utils.PinFileSimple(h.ipfsClusterClient, packfileName)
		if err != nil {
			logger.FromContext(ctx).WithError(err).WithField("packfile", packfileName).Error("failed to pin packfile")
			continue
		}
		
		logger.FromContext(ctx).WithFields(logrus.Fields{
			"repository_id": repositoryID,
			"packfile":      filepath.Base(packfileName),
			"cid":           cid,
		}).Info("pinned updated packfile")
	}

	logger.FromContext(ctx).WithField("repository_id", repositoryID).Info("successfully processed repository tag update")
	return nil
}

