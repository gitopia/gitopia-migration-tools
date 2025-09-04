package handler

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"

	"github.com/buger/jsonparser"
	gc "github.com/gitopia/gitopia-go"
	"github.com/gitopia/gitopia-go/logger"
	"github.com/gitopia/gitopia-migration-tools/shared"
	gitopiatypes "github.com/gitopia/gitopia/v5/x/gitopia/types"
	ipfsclusterclient "github.com/ipfs-cluster/ipfs-cluster/api/rest/client"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
)

// ExtractStringArray extracts an array of strings from JSON event data
func ExtractStringArray(eventBuf []byte, path ...string) ([]string, error) {
	var result []string

	// First try to get as array
	arrayData, dataType, _, err := jsonparser.Get(eventBuf, path...)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to extract %v", path)
	}

	switch dataType {
	case jsonparser.Array:
		// Parse as array
		_, err = jsonparser.ArrayEach(arrayData, func(value []byte, dataType jsonparser.ValueType, offset int, err error) {
			if err != nil {
				return
			}
			if dataType == jsonparser.String {
				str, _ := jsonparser.ParseString(value)
				result = append(result, str)
			}
		})
		if err != nil {
			return nil, errors.Wrapf(err, "failed to parse array for %v", path)
		}
	case jsonparser.String:
		// Single string value
		str, err := jsonparser.ParseString(arrayData)
		if err != nil {
			return nil, errors.Wrapf(err, "failed to parse string for %v", path)
		}
		result = append(result, str)
	default:
		return nil, fmt.Errorf("unexpected data type for %v: %v", path, dataType)
	}

	return result, nil
}

// LoadParentRepository loads a parent repository for forked repos
func LoadParentRepository(ctx context.Context, parentRepoID uint64, gitDir string, gitopiaClient gc.Client, ipfsClusterClient ipfsclusterclient.Client, storageManager *shared.StorageManager) error {
	// Check if parent packfile exists in storage manager
	parentPackfile := storageManager.GetPackfileInfo(parentRepoID)
	if parentPackfile == nil {
		return errors.Errorf("parent repository %d packfile not found in storage", parentRepoID)
	}

	// Get parent repository info
	parentRepository, err := gitopiaClient.QueryClient().Gitopia.Repository(ctx, &gitopiatypes.QueryGetRepositoryRequest{
		Id: parentRepoID,
	})
	if err != nil {
		return errors.Wrap(err, "failed to get parent repository")
	}

	// Check if parent repository is empty
	branch, err := gitopiaClient.QueryClient().Gitopia.RepositoryBranch(ctx, &gitopiatypes.QueryGetRepositoryBranchRequest{
		Id:             parentRepository.Repository.Owner.Id,
		RepositoryName: parentRepository.Repository.Name,
		BranchName:     parentRepository.Repository.DefaultBranch,
	})
	if err != nil {
		return errors.Wrapf(err, "error getting parent repository branches for repo %d", parentRepoID)
	}
	if branch.Branch.Name == "" {
		logger.FromContext(ctx).WithField("parent_id", parentRepoID).Info("parent repository is empty, skipping")
		return nil
	}

	// Initialize parent repository directory
	parentRepoDir := filepath.Join(gitDir, fmt.Sprintf("%d.git", parentRepoID))
	if _, err := os.Stat(filepath.Join(parentRepoDir, "objects")); os.IsNotExist(err) {
		cmd := exec.Command("git", "init", "--bare", parentRepoDir)
		if err := cmd.Run(); err != nil {
			return errors.Wrapf(err, "failed to initialize parent repository %d", parentRepoID)
		}
	}

	// Download packfile from IPFS cluster using CID from storage
	if err := downloadPackfileFromIPFSCluster(parentPackfile.CID, parentPackfile.Name, parentRepoDir); err != nil {
		return errors.Wrapf(err, "error downloading parent packfile for repo %d", parentRepoID)
	}

	// Recursively load parent's parent if it's also a fork
	if parentRepository.Repository.Fork {
		grandParentRepoDir := filepath.Join(gitDir, fmt.Sprintf("%d.git", parentRepository.Repository.Parent))
		if _, err := os.Stat(grandParentRepoDir); os.IsNotExist(err) {
			if err := LoadParentRepository(ctx, parentRepository.Repository.Parent, gitDir, gitopiaClient, ipfsClusterClient, storageManager); err != nil {
				return errors.Wrapf(err, "error loading grandparent repository %d", parentRepository.Repository.Parent)
			}
		}
		
		// Set up alternates for parent repository
		if _, err := os.Stat(grandParentRepoDir); err == nil {
			alternatesPath := filepath.Join(parentRepoDir, "objects", "info", "alternates")
			if err := os.MkdirAll(filepath.Dir(alternatesPath), 0755); err != nil {
				return errors.Wrapf(err, "error creating alternates directory for parent repo %d", parentRepoID)
			}
			grandParentObjectsPath := filepath.Join(grandParentRepoDir, "objects")
			if err := os.WriteFile(alternatesPath, []byte(grandParentObjectsPath+"\n"), 0644); err != nil {
				return errors.Wrapf(err, "error creating alternates file for parent repo %d", parentRepoID)
			}
			logger.FromContext(ctx).WithFields(logrus.Fields{
				"parent_id": parentRepoID,
				"grandparent_id": parentRepository.Repository.Parent,
			}).Info("created alternates file linking parent repository to grandparent")
		}
	}

	// Run git gc on parent repository
	cmd := exec.Command("git", "gc")
	cmd.Dir = parentRepoDir
	if err := cmd.Run(); err != nil {
		return errors.Wrapf(err, "error running git gc for parent repo %d", parentRepoID)
	}

	// Parent packfile is already stored in IPFS cluster, no need to pin again
	logger.FromContext(ctx).WithFields(logrus.Fields{
		"parent_id": parentRepoID,
		"cid": parentPackfile.CID,
	}).Info("successfully loaded parent repository from IPFS")

	return nil
}

// downloadPackfileFromIPFSCluster downloads a packfile from IPFS cluster and sets up the repository
func downloadPackfileFromIPFSCluster(cid, packfileName, repoDir string) error {
	// Download packfile from IPFS cluster
	packDir := filepath.Join(repoDir, "objects", "pack")
	if err := os.MkdirAll(packDir, 0755); err != nil {
		return errors.Wrap(err, "failed to create pack directory")
	}

	packfilePath := filepath.Join(packDir, packfileName)
	if err := downloadFromIPFSClusterHTTP(cid, packfilePath); err != nil {
		return errors.Wrap(err, "failed to download packfile from IPFS")
	}

	// Build pack index file
	cmd := exec.Command("git", "index-pack", packfilePath)
	cmd.Dir = repoDir
	if err := cmd.Run(); err != nil {
		return errors.Wrap(err, "failed to build pack index")
	}

	return nil
}

// downloadFromIPFSClusterHTTP downloads a file from IPFS cluster using HTTP API
func downloadFromIPFSClusterHTTP(cid, filePath string) error {
	ipfsUrl := fmt.Sprintf("http://%s:%s/api/v0/cat?arg=/ipfs/%s&progress=false", 
		viper.GetString("IPFS_CLUSTER_PEER_HOST"), 
		viper.GetString("IPFS_CLUSTER_PEER_PORT"), 
		cid)
	
	resp, err := http.Post(ipfsUrl, "application/json", nil)
	if err != nil {
		return fmt.Errorf("failed to fetch file from IPFS: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("failed to fetch file from IPFS: %v", resp.Status)
	}

	file, err := os.Create(filePath)
	if err != nil {
		return fmt.Errorf("failed to create file: %v", err)
	}
	defer file.Close()

	if _, err := io.Copy(file, resp.Body); err != nil {
		return fmt.Errorf("failed to write file: %v", err)
	}

	return nil
}
