package shared

import (
	"crypto/sha256"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/ipfs/boxo/files"
	"github.com/pkg/errors"
	merkletree "github.com/wealdtech/go-merkletree/v2"
	"github.com/wealdtech/go-merkletree/v2/sha3"
)

// ComputeFileInfo computes CID, merkle root, and size for a file
func ComputeFileInfo(filePath, cid string) ([]byte, int64, error) {
	// Get file info
	fileInfo, err := os.Stat(filePath)
	if err != nil {
		return nil, 0, errors.Wrapf(err, "failed to get file info for %s", filePath)
	}

	// Open file for merkle root calculation
	file, err := os.Open(filePath)
	if err != nil {
		return nil, 0, errors.Wrapf(err, "failed to open file %s", filePath)
	}
	defer file.Close()

	// Create files.File from os.File
	ipfsFile, err := files.NewReaderPathFile(filePath, file, fileInfo)
	if err != nil {
		return nil, 0, errors.Wrapf(err, "failed to create files.File for %s", filePath)
	}

	// Compute merkle root
	rootHash, err := ComputeMerkleRoot(ipfsFile)
	if err != nil {
		return nil, 0, errors.Wrapf(err, "failed to compute merkle root for %s", filePath)
	}

	return rootHash, fileInfo.Size(), nil
}

// GetPackfilePath returns the path to the packfile for a repository
func GetPackfilePath(gitDir string, repositoryID uint64) (string, error) {
	repoDir := filepath.Join(gitDir, fmt.Sprintf("%d.git", repositoryID))
	packDir := filepath.Join(repoDir, "objects", "pack")
	packFiles, err := filepath.Glob(filepath.Join(packDir, "*.pack"))
	if err != nil {
		return "", errors.Wrapf(err, "error finding packfiles for repo %d", repositoryID)
	}
	if len(packFiles) == 0 {
		return "", errors.Errorf("no packfiles found for repo %d", repositoryID)
	}
	return packFiles[0], nil
}

// GetLFSObjectPath returns the path to an LFS object
func GetLFSObjectPath(gitDir string, repositoryID uint64, oid string) string {
	repoDir := filepath.Join(gitDir, fmt.Sprintf("%d.git", repositoryID))
	lfsObjectsDir := filepath.Join(repoDir, "lfs", "objects")
	return filepath.Join(lfsObjectsDir, oid[:2], oid[2:])
}

// GetAttachmentPath returns the path to a release attachment
func GetAttachmentPath(attachmentDir, sha256 string) string {
	return filepath.Join(attachmentDir, sha256)
}

const (
	// DefaultChunkSize defines the default chunk size for merkle root calculation.
	DefaultChunkSize = 256 * 1024
)

// computeChunkHashes computes the sha256 hash of each chunk of a file.
func computeChunkHashes(file files.File, chunkSize int) ([][]byte, error) {
	// Get file size
	fileSize, err := file.Size()
	if err != nil {
		return nil, fmt.Errorf("failed to get file size: %w", err)
	}

	// Calculate how many chunks we'll have
	numChunks := (fileSize + int64(chunkSize) - 1) / int64(chunkSize)
	if numChunks == 0 {
		return nil, fmt.Errorf("empty file")
	}

	// Pre-compute hashes by reading one chunk at a time
	chunkHashes := make([][]byte, 0, numChunks)
	buffer := make([]byte, chunkSize)
	hasher := sha256.New()

	for offset := int64(0); offset < fileSize; {
		// Calculate actual chunk size (might be smaller for the last chunk)
		currentChunkSize := int(min(int64(chunkSize), fileSize-offset))

		// Reset buffer slice to exact size needed
		chunk := buffer[:currentChunkSize]

		// Read chunk
		n, err := io.ReadFull(file, chunk)
		if err != nil && err != io.EOF {
			return nil, fmt.Errorf("failed to read chunk at offset %d: %w", offset, err)
		}
		if n != currentChunkSize {
			return nil, fmt.Errorf("short read: got %d bytes, expected %d", n, currentChunkSize)
		}

		// Hash the chunk on the fly
		hasher.Reset()
		hasher.Write(chunk)
		hash := hasher.Sum(nil)
		chunkHashes = append(chunkHashes, hash)

		// Move to next chunk
		offset += int64(currentChunkSize)
	}
	return chunkHashes, nil
}

// ComputeMerkleRoot calculates the Merkle root of a file by:
// 1. Reading one chunk at a time to minimize memory usage
// 2. Using the hash of each chunk as a leaf node
// 3. Building the Merkle tree only from hashes
func ComputeMerkleRoot(file files.File) ([]byte, error) {
	chunkHashes, err := computeChunkHashes(file, DefaultChunkSize)
	if err != nil {
		return nil, fmt.Errorf("failed to compute chunk hashes: %w", err)
	}

	// Create Merkle tree from the hashes (directly use hashes as data)
	tree, err := merkletree.NewTree(
		merkletree.WithData(chunkHashes),
		merkletree.WithHashType(sha3.New256()),
		merkletree.WithSalt(false),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create merkle tree: %w", err)
	}

	// Return the root
	return tree.Root(), nil
}

// GenerateProof generates a proof for a specific chunk in the file
func GenerateProof(file files.File, chunkIndex uint64) (*merkletree.Proof, []byte, []byte, error) {
	chunkHashes, err := computeChunkHashes(file, DefaultChunkSize)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("failed to compute chunk hashes: %w", err)
	}

	// Create Merkle tree from the hashes
	tree, err := merkletree.NewTree(
		merkletree.WithData(chunkHashes),
		merkletree.WithHashType(sha3.New256()),
		merkletree.WithSalt(false),
	)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("failed to create merkle tree: %w", err)
	}

	// Generate proof for the target chunk hash
	proof, err := tree.GenerateProof(chunkHashes[chunkIndex], 0)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("failed to generate proof for chunk %d: %w", chunkIndex, err)
	}

	return proof, tree.Root(), chunkHashes[chunkIndex], nil
}

// VerifyProof verifies a single chunk proof against the Merkle root
func VerifyProof(chunkHash []byte, proof *merkletree.Proof, root []byte) (bool, error) {
	if proof == nil {
		return false, fmt.Errorf("nil proof")
	}

	// Verify the proof against the root
	verified, err := merkletree.VerifyProofUsing(
		chunkHash,
		false, // No salting
		proof,
		[][]byte{root},
		sha3.New256(),
	)
	if err != nil {
		return false, fmt.Errorf("failed to verify proof: %w", err)
	}

	return verified, nil
}
