# Update Script

This component updates packfile, LFS objects and release assets transactions on the Gitopia blockchain after the v6 upgrade.

## Purpose

- Updates blockchain with storage information for all repositories
- Computes and submits merkle roots for verification
- Handles packfiles, LFS objects, and release assets
- Uses Gitopia v6.0.0-rc.6 and gitopia-go v0.7.0-rc.3 for post-upgrade compatibility

## Prerequisites

- IPFS Kubo v0.25+
- Access to Gitopia v6.0.0-rc.6 blockchain
- Previously cloned repositories and assets (from clone-script)
- Storage provider registered on blockchain
- Sufficient balance for transaction fees

## Configuration

1. Copy the example configuration:
   ```bash
   cp config.toml.example config.toml
   ```

2. Edit `config.toml` with your settings:
   - `GIT_REPOS_DIR`: Directory containing cloned repositories
   - `ATTACHMENT_DIR`: Directory containing release assets
   - `GITOPIA_ADDR`: Gitopia v6 gRPC endpoint
   - `TM_ADDR`: Tendermint RPC endpoint
   - `IPFS_HOST/PORT`: IPFS API connection

## Usage

1. **Setup wallet key:**
   ```bash
   ./update keys add gitopia-storage --keyring-backend test --recover
   ```

2. **Run the update script:**
   ```bash
   ./update --from gitopia-storage --keyring-backend test --fees 200ulore
   ```

## Process Flow

### Repository Updates
1. Iterates through all repositories on blockchain
2. Locates local repository data
3. Retrieves CID from IPFS cluster
4. Computes merkle root for verification
5. Submits `MsgUpdatePackfile` transaction
6. Processes LFS objects with `MsgUpdateLFSObject`
7. Verifies updates were applied

### Release Asset Updates
1. Processes all releases with attachments
2. Locates local asset files
3. Retrieves CIDs from IPFS cluster
4. Computes merkle roots
5. Submits batch `MsgUpdateReleaseAssets` transaction
6. Verifies updates were applied

## Features

- **Progress tracking**: Maintains progress in `update_progress.json`
- **Resume capability**: Can resume from failures
- **Batch processing**: Efficient transaction batching
- **Verification**: Polls blockchain to confirm updates
- **Error handling**: Tracks and retries failed items

## Transaction Types

### MsgUpdatePackfile
- Repository ID
- Packfile name and CID
- Merkle root hash
- File size
- Old CID (for updates)

### MsgUpdateLFSObject
- Repository ID
- LFS object OID
- CID and merkle root
- File size

### MsgUpdateReleaseAssets
- Repository ID and tag name
- Array of asset updates
- Each asset: name, CID, merkle root, size, SHA256

## Error Handling

- Failed repositories and releases tracked in progress file
- Detailed error logging with repository/release IDs
- Script can be restarted to retry failed items
- Transaction verification with timeout handling

## Build

```bash
go mod download
go build -o update ./cmd/update
```

## Important Notes

- Run only after v6 upgrade is complete
- Ensure storage provider is registered first
- Monitor transaction fees and account balance
- Large repositories may take significant time to process
