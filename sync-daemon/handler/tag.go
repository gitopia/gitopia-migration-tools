package handler

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"time"

	gc "github.com/gitopia/gitopia-go"
	"github.com/gitopia/gitopia-go/logger"
	"github.com/gitopia/gitopia-migration-tools/shared"
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
	storageManager    *shared.StorageManager
	pinataClient      *shared.PinataClient
}

func NewTagEventHandler(gitopiaClient gc.Client, ipfsClusterClient ipfsclusterclient.Client, ipfsHttpApi *rpc.HttpApi, storageManager *shared.StorageManager, pinataClient *shared.PinataClient) *TagEventHandler {
	return &TagEventHandler{
		gitopiaClient:     gitopiaClient,
		ipfsClusterClient: ipfsClusterClient,
		ipfsHttpApi:       ipfsHttpApi,
		storageManager:    storageManager,
		pinataClient:      pinataClient,
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

		// Handle forked repository optimization
		if repository.Repository.Fork {
			logger.FromContext(ctx).WithFields(logrus.Fields{
				"repository_id": repositoryID,
				"parent_id":     repository.Repository.Parent,
			}).Info("setting up alternates for forked repository")

			// Load parent repository if it doesn't exist
			parentRepoDir := filepath.Join(gitDir, fmt.Sprintf("%d.git", repository.Repository.Parent))
			if _, err := os.Stat(parentRepoDir); os.IsNotExist(err) {
				if err := LoadParentRepository(ctx, repository.Repository.Parent, gitDir, h.gitopiaClient, h.ipfsClusterClient, h.storageManager); err != nil {
					return errors.Wrapf(err, "error loading parent repository %d", repository.Repository.Parent)
				}
			}
			
			if _, err := os.Stat(parentRepoDir); err == nil {
				// Create alternates file to link with parent repo
				alternatesPath := filepath.Join(repoDir, "objects", "info", "alternates")
				if err := os.MkdirAll(filepath.Dir(alternatesPath), 0755); err != nil {
					return errors.Wrapf(err, "error creating alternates directory for repo %d", repositoryID)
				}

				parentObjectsPath := filepath.Join(parentRepoDir, "objects")
				if err := os.WriteFile(alternatesPath, []byte(parentObjectsPath+"\n"), 0644); err != nil {
					return errors.Wrapf(err, "error creating alternates file for repo %d", repositoryID)
				}
				logger.FromContext(ctx).WithField("parent_id", repository.Repository.Parent).Info("created alternates file linking to parent repository")
			} else {
				logger.FromContext(ctx).WithField("parent_id", repository.Repository.Parent).Warn("parent repository not found, alternates not created")
			}
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

	// For forked repos with alternates, run git repack to remove common objects
	if repository.Repository.Fork {
		alternatesPath := filepath.Join(repoDir, "objects", "info", "alternates")
		if _, err := os.Stat(alternatesPath); err == nil {
			logger.FromContext(ctx).WithField("repository_id", repositoryID).Info("running git repack to optimize forked repository")
			cmd = exec.Command("git", "repack", "-a", "-d", "-l")
			cmd.Dir = repoDir
			if err := cmd.Run(); err != nil {
				logger.FromContext(ctx).WithError(err).WithField("repository_id", repositoryID).Warn("git repack failed for forked repo")
			}
		}
	}

	// Pin packfile to IPFS cluster
	packDir := filepath.Join(repoDir, "objects", "pack")
	packFiles, err := filepath.Glob(filepath.Join(packDir, "*.pack"))
	if err != nil {
		return errors.Wrapf(err, "error finding packfiles for repo %d", repositoryID)
	}

	if len(packFiles) > 0 {
		packfilePath := packFiles[0]
		cid, err := shared.PinFile(h.ipfsClusterClient, packfilePath)
		if err != nil {
			return errors.Wrap(err, "error pinning packfile")
		}

		// Compute merkle root and file info
		rootHash, size, err := shared.ComputeFileInfo(packfilePath, cid)
		if err != nil {
			logger.FromContext(ctx).WithError(err).WithField("repository_id", repositoryID).Error("failed to compute file info for packfile")
		} else {
			// Get existing packfile info to unpin older version
			existingPackfile := h.storageManager.GetPackfileInfo(repositoryID)
			
			// Store packfile information (sync daemon takes precedence)
			packfileInfo := &shared.PackfileInfo{
				RepositoryID: repositoryID,
				CID:          cid,
				RootHash:     rootHash,
				Size:         size,
				UpdatedAt:    time.Now(),
				UpdatedBy:    "sync",
			}
			h.storageManager.SetPackfileInfo(packfileInfo)
			if err := h.storageManager.Save(); err != nil {
				logger.FromContext(ctx).WithError(err).Error("failed to save storage manager")
			}
			
			// Pin to Pinata if enabled
			if h.pinataClient != nil {
				resp, err := h.pinataClient.PinFile(ctx, packfilePath, filepath.Base(packfilePath))
				if err != nil {
					logger.FromContext(ctx).WithError(err).WithField("repository_id", repositoryID).Error("failed to pin packfile to Pinata")
				} else {
					logger.FromContext(ctx).WithFields(logrus.Fields{
						"repository_id": repositoryID,
						"packfile":      filepath.Base(packfilePath),
						"pinata_id":     resp.Data.ID,
					}).Info("successfully pinned packfile to Pinata")
				}
			}
			
			// Unpin older version if it exists and is different
			if existingPackfile != nil && existingPackfile.CID != cid {
				if err := shared.UnpinFile(h.ipfsClusterClient, existingPackfile.CID); err != nil {
					logger.FromContext(ctx).WithError(err).WithFields(logrus.Fields{
						"repository_id": repositoryID,
						"old_cid":       existingPackfile.CID,
					}).Warn("failed to unpin older packfile version")
				} else {
					logger.FromContext(ctx).WithFields(logrus.Fields{
						"repository_id": repositoryID,
						"old_cid":       existingPackfile.CID,
					}).Info("successfully unpinned older packfile version")
				}
			}
			// Delete the repository directory after successful pinning and storage
			if err := os.RemoveAll(repoDir); err != nil {
				logger.FromContext(ctx).WithError(err).WithField("repo_dir", repoDir).Warn("failed to delete repository directory")
			} else {
				logger.FromContext(ctx).WithField("repository_id", repositoryID).Info("successfully deleted repository directory")
			}
		}

		logger.FromContext(ctx).WithFields(logrus.Fields{
			"repository_id": repositoryID,
			"cid":           cid,
		}).Info("pinned packfile")

	} else if repository.Repository.Fork {
		logger.FromContext(ctx).WithField("repository_id", repositoryID).Info("forked repository has no packfile, using parent objects via alternates")
		// Delete the repository directory after processing fork
		if err := os.RemoveAll(repoDir); err != nil {
			logger.FromContext(ctx).WithError(err).WithField("repo_dir", repoDir).Warn("failed to delete forked repository directory")
		} else {
			logger.FromContext(ctx).WithField("repository_id", repositoryID).Info("successfully deleted forked repository directory")
		}
	}

	logger.FromContext(ctx).WithField("repository_id", repositoryID).Info("successfully processed repository tag update")
	return nil
}

