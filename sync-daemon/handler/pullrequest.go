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

type PullRequestEventHandler struct {
	gitopiaClient     gc.Client
	ipfsClusterClient ipfsclusterclient.Client
	ipfsHttpApi       *rpc.HttpApi
	storageManager    *shared.StorageManager
	pinataClient      *shared.PinataClient
}

func NewPullRequestEventHandler(gitopiaClient gc.Client, ipfsClusterClient ipfsclusterclient.Client, ipfsHttpApi *rpc.HttpApi, storageManager *shared.StorageManager, pinataClient *shared.PinataClient) *PullRequestEventHandler {
	return &PullRequestEventHandler{
		gitopiaClient:     gitopiaClient,
		ipfsClusterClient: ipfsClusterClient,
		ipfsHttpApi:       ipfsHttpApi,
		storageManager:    storageManager,
		pinataClient:      pinataClient,
	}
}

func (h *PullRequestEventHandler) Handle(ctx context.Context, eventBuf []byte) error {
	// Extract pull request state from the event
	states, err := ExtractStringArray(eventBuf, "message", gitopiatypes.EventAttributePullRequestStateKey)
	if err != nil {
		return errors.Wrap(err, "error parsing pull request state")
	}

	// Only process merged pull requests
	for _, state := range states {
		if state != "MERGED" {
			logger.FromContext(ctx).WithField("state", state).Debug("skipping non-merged pull request state")
			continue
		}

		// Extract repository ID from the event
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

			// Extract pull request ID for logging
			prIDs, err := ExtractStringArray(eventBuf, "message", gitopiatypes.EventAttributePullRequestIdKey)
			if err != nil {
				logger.FromContext(ctx).WithError(err).Warn("failed to extract pull request ID")
			}

			prID := "unknown"
			if len(prIDs) > 0 {
				prID = prIDs[0]
			}

			logger.FromContext(ctx).WithFields(logrus.Fields{
				"repository_id":   repoID,
				"pull_request_id": prID,
				"state":           state,
			}).Info("processing merged pull request")

			if err := h.processRepositoryAfterMerge(ctx, repoID); err != nil {
				logger.FromContext(ctx).WithError(err).WithFields(logrus.Fields{
					"repository_id":   repoID,
					"pull_request_id": prID,
				}).Error("failed to process repository after pull request merge")
			}
		}
	}

	return nil
}

func (h *PullRequestEventHandler) processRepositoryAfterMerge(ctx context.Context, repositoryID uint64) error {
	logger.FromContext(ctx).WithField("repository_id", repositoryID).Info("processing repository after pull request merge")

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
			}
		}
	} else {
		// Update existing repository to get the latest merged changes
		logger.FromContext(ctx).WithField("repository_id", repositoryID).Info("updating repository after merge")

		cmd := exec.Command("git", "fetch", "--all")
		cmd.Dir = repoDir
		if err := cmd.Run(); err != nil {
			return errors.Wrapf(err, "error fetching updates for repository %d", repositoryID)
		}
	}

	// Run git gc to optimize and create new packfiles after merge
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

	if len(packFiles) > 0 {
		packfilePath := packFiles[0]
		cid, err := shared.PinFile(h.ipfsClusterClient, packfilePath)
		if err != nil {
			logger.FromContext(ctx).WithError(err).WithField("packfile", packfilePath).Error("failed to pin packfile after merge")
		} else {
			// Compute merkle root and file info
			rootHash, size, err := shared.ComputeFileInfo(packfilePath, cid)
			if err != nil {
				logger.FromContext(ctx).WithError(err).WithField("repository_id", repositoryID).Error("failed to compute file info for packfile")
			} else {
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
						logger.FromContext(ctx).WithError(err).WithField("repository_id", repositoryID).Error("failed to pin packfile to Pinata after merge")
					} else {
						logger.FromContext(ctx).WithFields(logrus.Fields{
							"repository_id": repositoryID,
							"packfile":      filepath.Base(packfilePath),
							"pinata_id":     resp.Data.ID,
						}).Info("successfully pinned packfile to Pinata after merge")
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
				"packfile":      filepath.Base(packfilePath),
				"cid":           cid,
			}).Info("pinned updated packfile after pull request merge")
		}
	} else if repository.Repository.Fork {
		// Delete the repository directory after processing fork
		if err := os.RemoveAll(repoDir); err != nil {
			logger.FromContext(ctx).WithError(err).WithField("repo_dir", repoDir).Warn("failed to delete forked repository directory")
		} else {
			logger.FromContext(ctx).WithField("repository_id", repositoryID).Info("successfully deleted forked repository directory")
		}
	}

	logger.FromContext(ctx).WithField("repository_id", repositoryID).Info("successfully processed repository after pull request merge")
	return nil
}

