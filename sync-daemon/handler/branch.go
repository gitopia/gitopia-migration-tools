package handler

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"

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

type BranchEventHandler struct {
	gitopiaClient     gc.Client
	ipfsClusterClient ipfsclusterclient.Client
	ipfsHttpApi       *rpc.HttpApi
}

func NewBranchEventHandler(gitopiaClient gc.Client, ipfsClusterClient ipfsclusterclient.Client, ipfsHttpApi *rpc.HttpApi) *BranchEventHandler {
	return &BranchEventHandler{
		gitopiaClient:     gitopiaClient,
		ipfsClusterClient: ipfsClusterClient,
		ipfsHttpApi:       ipfsHttpApi,
	}
}

func (h *BranchEventHandler) Handle(ctx context.Context, eventBuf []byte) error {
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
			logger.FromContext(ctx).WithError(err).WithField("repository_id", repoID).Error("failed to process repository branch update")
		}
	}

	return nil
}

func (h *BranchEventHandler) processRepository(ctx context.Context, repositoryID uint64) error {
	logger.FromContext(ctx).WithField("repository_id", repositoryID).Info("processing repository branch update")

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
		logger.FromContext(ctx).WithField("repository_id", repositoryID).Info("updating existing repository")
		
		cmd := exec.Command("git", "fetch", "--all")
		cmd.Dir = repoDir
		if err := cmd.Run(); err != nil {
			return errors.Wrapf(err, "error fetching updates for repository %d", repositoryID)
		}
	}

	// Run git gc to optimize and create new packfiles
	cmd := exec.Command("git", "gc", "--aggressive")
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

	// Process LFS objects if they exist
	if err := h.processLFSObjects(ctx, repositoryID, repoDir); err != nil {
		logger.FromContext(ctx).WithError(err).WithField("repository_id", repositoryID).Error("failed to process LFS objects")
	}

	logger.FromContext(ctx).WithField("repository_id", repositoryID).Info("successfully processed repository branch update")
	return nil
}

func (h *BranchEventHandler) processLFSObjects(ctx context.Context, repositoryID uint64, repoDir string) error {
	lfsObjectsDir := filepath.Join(repoDir, "lfs", "objects")

	if _, err := os.Stat(lfsObjectsDir); os.IsNotExist(err) {
		return nil // No LFS objects
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

	for _, oid := range lfsObjects {
		oidPath := filepath.Join(lfsObjectsDir, oid[:2], oid[2:])

		cid, err := utils.PinFileSimple(h.ipfsClusterClient, oidPath)
		if err != nil {
			logger.FromContext(ctx).WithError(err).WithFields(logrus.Fields{
				"repository_id": repositoryID,
				"oid":           oid,
			}).Error("failed to pin LFS object")
			continue
		}

		logger.FromContext(ctx).WithFields(logrus.Fields{
			"repository_id": repositoryID,
			"oid":           oid,
			"cid":           cid,
		}).Info("pinned LFS object")
	}

	return nil
}

