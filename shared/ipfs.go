package shared

import (
	"context"
	"fmt"
	"path/filepath"

	"github.com/ipfs-cluster/ipfs-cluster/api"
	ipfsclusterclient "github.com/ipfs-cluster/ipfs-cluster/api/rest/client"
)

func PinFile(ipfsClusterClient ipfsclusterclient.Client, filePath string) (string, error) {
	paths := []string{filePath}
	addParams := api.DefaultAddParams()
	addParams.CidVersion = 1
	addParams.RawLeaves = true

	outputChan := make(chan api.AddedOutput)
	var cid api.Cid

	go func() {
		err := ipfsClusterClient.Add(context.Background(), paths, addParams, outputChan)
		if err != nil {
			close(outputChan)
		}
	}()

	// Get CID from output channel
	for output := range outputChan {
		cid = output.Cid
	}

	// Pin the file with default options
	pinOpts := api.PinOptions{
		ReplicationFactorMin: -1,
		ReplicationFactorMax: -1,
		Name:                 filepath.Base(filePath),
	}

	_, err := ipfsClusterClient.Pin(context.Background(), cid, pinOpts)
	if err != nil {
		return "", fmt.Errorf("failed to pin file in IPFS cluster: %w", err)
	}

	return cid.String(), nil
}

func UnpinFile(ipfsClusterClient ipfsclusterclient.Client, cidString string) error {
	cid, err := api.DecodeCid(cidString)
	if err != nil {
		return err
	}

	// unpin the packfile from IPFS cluster
	_, err = ipfsClusterClient.Unpin(context.Background(), cid)
	if err != nil {
		return err
	}
	return nil
}
