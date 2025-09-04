package handler

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	gc "github.com/gitopia/gitopia-go"
	"github.com/gitopia/gitopia-go/logger"
	"github.com/gitopia/gitopia-migration-tools/shared"
	gitopiatypes "github.com/gitopia/gitopia/v5/x/gitopia/types"
	ipfsclusterclient "github.com/ipfs-cluster/ipfs-cluster/api/rest/client"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
)

type ReleaseEvent struct {
	Creator           string
	RepositoryId      uint64
	RepositoryOwnerId string
	Tag               string
}

type ReleaseEventHandler struct {
	gitopiaClient     gc.Client
	ipfsClusterClient ipfsclusterclient.Client
	storageManager    *shared.StorageManager
	pinataClient      *shared.PinataClient
}

func NewReleaseEventHandler(gitopiaClient gc.Client, ipfsClusterClient ipfsclusterclient.Client, storageManager *shared.StorageManager, pinataClient *shared.PinataClient) *ReleaseEventHandler {
	return &ReleaseEventHandler{
		gitopiaClient:     gitopiaClient,
		ipfsClusterClient: ipfsClusterClient,
		storageManager:    storageManager,
		pinataClient:      pinataClient,
	}
}

func (h *ReleaseEventHandler) Handle(ctx context.Context, eventBuf []byte, eventType string) error {
	events, err := h.unmarshalReleaseEvent(eventBuf, eventType)
	if err != nil {
		return errors.WithMessage(err, "event parse error")
	}

	for _, event := range events {
		if err := h.processReleaseEvent(ctx, event, eventType); err != nil {
			logger.FromContext(ctx).WithFields(logrus.Fields{
				"repository_id": event.RepositoryId,
				"tag":           event.Tag,
				"event_type":    eventType,
			}).WithError(err).Error("failed to process ReleaseEvent")
		}
	}

	return nil
}

func (h *ReleaseEventHandler) unmarshalReleaseEvent(eventBuf []byte, eventType string) ([]ReleaseEvent, error) {
	var events []ReleaseEvent

	var creators []string
	var err error

	if eventType == "DaoCreateRelease" {
		creators, err = ExtractStringArray(eventBuf, "message", "sender")
	} else {
		creators, err = ExtractStringArray(eventBuf, "message", gitopiatypes.EventAttributeCreatorKey)
	}
	if err != nil {
		return nil, errors.Wrap(err, "error parsing creator")
	}

	repoIDs, err := ExtractStringArray(eventBuf, "message", gitopiatypes.EventAttributeRepoIdKey)
	if err != nil {
		return nil, errors.Wrap(err, "error parsing repository id")
	}

	repoOwnerIDs, err := ExtractStringArray(eventBuf, "message", gitopiatypes.EventAttributeRepoOwnerIdKey)
	if err != nil {
		return nil, errors.Wrap(err, "error parsing repository owner id")
	}

	tags, err := ExtractStringArray(eventBuf, "message", gitopiatypes.EventAttributeReleaseTagNameKey)
	if err != nil {
		return nil, errors.Wrap(err, "error parsing tag")
	}

	if len(creators) == 0 {
		return events, nil
	}

	if !(len(repoIDs) == len(repoOwnerIDs) && len(repoIDs) == len(tags)) {
		return nil, errors.New("mismatched attribute array lengths for ReleaseEvent")
	}

	for i := 0; i < len(repoIDs); i++ {
		repoId, err := strconv.ParseUint(repoIDs[i], 10, 64)
		if err != nil {
			return nil, errors.Wrap(err, "error parsing repository id")
		}

		events = append(events, ReleaseEvent{
			Creator:           creators[i],
			RepositoryId:      repoId,
			RepositoryOwnerId: repoOwnerIDs[i],
			Tag:               tags[i],
		})
	}

	return events, nil
}

func (h *ReleaseEventHandler) processReleaseEvent(ctx context.Context, event ReleaseEvent, eventType string) error {
	logger.FromContext(ctx).WithFields(logrus.Fields{
		"repository_id": event.RepositoryId,
		"tag":           event.Tag,
		"event_type":    eventType,
	}).Info("processing release event")

	attachmentDir := viper.GetString("ATTACHMENT_DIR")

	switch eventType {
	case "CreateRelease", "DaoCreateRelease", "UpdateRelease":
		// Query repository
		repository, err := h.gitopiaClient.QueryClient().Gitopia.Repository(ctx, &gitopiatypes.QueryGetRepositoryRequest{
			Id: event.RepositoryId,
		})
		if err != nil {
			return errors.Wrap(err, "error getting repository")
		}

		// Query release attachments
		release, err := h.gitopiaClient.QueryClient().Gitopia.RepositoryRelease(ctx, &gitopiatypes.QueryGetRepositoryReleaseRequest{
			Id:             repository.Repository.Owner.Id,
			RepositoryName: repository.Repository.Name,
			TagName:        event.Tag,
		})
		if err != nil {
			return errors.Wrap(err, "error getting release attachments")
		}

		// Download and pin all attachments
		for _, attachment := range release.Release.Attachments {
			if err := h.processAttachment(ctx, event, *attachment, attachmentDir); err != nil {
				logger.FromContext(ctx).WithError(err).WithFields(logrus.Fields{
					"attachment":    attachment.Name,
					"repository_id": event.RepositoryId,
					"tag":           event.Tag,
				}).Error("failed to process attachment")
			}
		}

	case "DeleteRelease":
		// For delete events, we don't need to download anything
		// The update script will handle unpinning later
		logger.FromContext(ctx).WithFields(logrus.Fields{
			"repository_id": event.RepositoryId,
			"tag":           event.Tag,
		}).Info("processed release delete event")
	}

	return nil
}

func (h *ReleaseEventHandler) processAttachment(ctx context.Context, event ReleaseEvent, attachment gitopiatypes.Attachment, attachmentDir string) error {
	// Get repository info
	repository, err := h.gitopiaClient.QueryClient().Gitopia.Repository(ctx, &gitopiatypes.QueryGetRepositoryRequest{
		Id: event.RepositoryId,
	})
	if err != nil {
		return errors.Wrap(err, "error getting repository")
	}

	// Download release asset
	attachmentUrl := fmt.Sprintf("%s/releases/%s/%s/%s/%s",
		viper.GetString("GIT_SERVER_HOST"),
		repository.Repository.Owner.Id,
		repository.Repository.Name,
		event.Tag,
		attachment.Name)

	filePath := filepath.Join(attachmentDir, attachment.Sha)

	// Check if file already exists
	if _, err := os.Stat(filePath); err == nil {
		logger.FromContext(ctx).WithFields(logrus.Fields{
			"attachment":    attachment.Name,
			"repository_id": event.RepositoryId,
			"tag":           event.Tag,
		}).Info("attachment already exists, skipping download")
	} else {
		// Download the file
		cmd := exec.Command("wget", attachmentUrl, "-O", filePath)
		output, err := cmd.CombinedOutput()
		if err != nil {
			return errors.Wrapf(err, "error downloading release asset: %s", string(output))
		}

		// Verify sha256
		cmd = exec.Command("sha256sum", filePath)
		output, err = cmd.Output()
		if err != nil {
			return errors.Wrap(err, "error verifying sha256 for attachment")
		}
		calculatedHash := strings.Fields(string(output))[0]
		if calculatedHash != attachment.Sha {
			return errors.Errorf("SHA256 mismatch for attachment %s: %s != %s", attachment.Name, calculatedHash, attachment.Sha)
		}

		logger.FromContext(ctx).WithFields(logrus.Fields{
			"attachment":    attachment.Name,
			"repository_id": event.RepositoryId,
			"tag":           event.Tag,
		}).Info("downloaded attachment")
	}

	// Pin to IPFS cluster
	cid, err := shared.PinFile(h.ipfsClusterClient, filePath)
	if err != nil {
		return errors.Wrap(err, "error pinning attachment")
	}

	// Compute merkle root and file info
	rootHash, size, err := shared.ComputeFileInfo(filePath, cid)
	if err != nil {
		logger.FromContext(ctx).WithError(err).WithField("attachment", attachment.Name).Error("failed to compute file info for attachment")
	} else {
		// Get existing release asset info to unpin older version
		existingAsset := h.storageManager.GetReleaseAssetInfo(event.RepositoryId, event.Tag, attachment.Name)
		
		// Store release asset information (sync daemon takes precedence)
		assetInfo := &shared.ReleaseAssetInfo{
			RepositoryID: event.RepositoryId,
			TagName:      event.Tag,
			Name:         attachment.Name,
			CID:          cid,
			RootHash:     rootHash,
			Size:         size,
			SHA256:       attachment.Sha,
			UpdatedAt:    time.Now(),
			UpdatedBy:    "sync",
		}
		h.storageManager.SetReleaseAssetInfo(assetInfo)
		if err := h.storageManager.Save(); err != nil {
			logger.FromContext(ctx).WithError(err).Error("failed to save storage manager")
		}
		
		// Pin to Pinata if enabled
		if h.pinataClient != nil {
			name := fmt.Sprintf("release-%d-%s-%s-%s", event.RepositoryId, event.Tag, attachment.Name, attachment.Sha)
			resp, err := h.pinataClient.PinFile(ctx, filePath, name)
			if err != nil {
				logger.FromContext(ctx).WithError(err).WithFields(logrus.Fields{
					"repository_id": event.RepositoryId,
					"tag":           event.Tag,
					"attachment":    attachment.Name,
				}).Error("failed to pin release asset to Pinata")
			} else {
				logger.FromContext(ctx).WithFields(logrus.Fields{
					"repository_id": event.RepositoryId,
					"tag":           event.Tag,
					"attachment":    attachment.Name,
					"pinata_id":     resp.Data.ID,
				}).Info("successfully pinned release asset to Pinata")
			}
		}
		
		// Unpin older version if it exists and is different
		if existingAsset != nil && existingAsset.CID != cid {
			if err := shared.UnpinFile(h.ipfsClusterClient, existingAsset.CID); err != nil {
				logger.FromContext(ctx).WithError(err).WithFields(logrus.Fields{
					"repository_id": event.RepositoryId,
					"tag":           event.Tag,
					"attachment":    attachment.Name,
					"old_cid":       existingAsset.CID,
				}).Warn("failed to unpin older release asset version from IPFS cluster")
			} else {
				logger.FromContext(ctx).WithFields(logrus.Fields{
					"repository_id": event.RepositoryId,
					"tag":           event.Tag,
					"attachment":    attachment.Name,
					"old_cid":       existingAsset.CID,
				}).Info("successfully unpinned older release asset version from IPFS cluster")
			}
			
			// Unpin from Pinata if enabled
			if h.pinataClient != nil {
				oldName := fmt.Sprintf("release-%d-%s-%s-%s", event.RepositoryId, event.Tag, attachment.Name, existingAsset.SHA256)
				if err := h.pinataClient.UnpinFile(ctx, oldName); err != nil {
					logger.FromContext(ctx).WithError(err).WithFields(logrus.Fields{
						"repository_id": event.RepositoryId,
						"tag":           event.Tag,
						"attachment":    attachment.Name,
						"old_cid":       existingAsset.CID,
					}).Warn("failed to unpin older release asset version from Pinata")
				} else {
					logger.FromContext(ctx).WithFields(logrus.Fields{
						"repository_id": event.RepositoryId,
						"tag":           event.Tag,
						"attachment":    attachment.Name,
						"old_cid":       existingAsset.CID,
					}).Info("successfully unpinned older release asset version from Pinata")
				}
			}
		}

		// Delete the attachment file after successful pinning and storage
		if err := os.Remove(filePath); err != nil {
			logger.FromContext(ctx).WithError(err).WithField("file_path", filePath).Warn("failed to delete attachment file")
		} else {
			logger.FromContext(ctx).WithField("file_path", filePath).Info("successfully deleted attachment file")
		}
	}

	logger.FromContext(ctx).WithFields(logrus.Fields{
		"attachment":    attachment.Name,
		"repository_id": event.RepositoryId,
		"tag":           event.Tag,
		"cid":           cid,
	}).Info("pinned release attachment")

	return nil
}
