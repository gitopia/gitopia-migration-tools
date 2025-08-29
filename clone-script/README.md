# Clone Script

This component clones all existing repositories (including LFS objects) and release assets from the Gitopia blockchain before the v6 upgrade.

## Purpose

- Downloads and pins all existing repository data to IPFS cluster
- Processes Git repositories, LFS objects, and release assets
- Uses Gitopia v5.1.0 and gitopia-go v0.6.2 for compatibility with pre-upgrade chain

## Prerequisites

- IPFS Kubo v0.25+
- IPFS Cluster v1.0+
- git-remote-gitopia configured
- Access to Gitopia v5.1.0 blockchain
- Sufficient disk space for all repositories and assets

## Configuration

1. Copy the example configuration:
   ```bash
   cp config.toml.example config.toml
   ```

2. Edit `config.toml` with your settings:
   - `GIT_REPOS_DIR`: Directory to store cloned repositories
   - `ATTACHMENT_DIR`: Directory to store release assets
   - `GITOPIA_ADDR`: Gitopia gRPC endpoint
   - `TM_ADDR`: Tendermint RPC endpoint
   - `IPFS_CLUSTER_PEER_HOST/PORT`: IPFS cluster connection
   - `GIT_SERVER_HOST`: Git server URL for downloading assets

## Usage

1. **Setup wallet key:**
   ```bash
   ./clone keys add gitopia-storage --keyring-backend test --recover
   ```

2. **Run the clone script:**
   ```bash
   ./clone --from gitopia-storage --keyring-backend test
   ```

## Features

- **Progress tracking**: Maintains progress in `clone_progress.json`
- **Resume capability**: Can resume from failures
- **LFS support**: Processes Git LFS objects
- **Verification**: Validates SHA256 checksums for downloads
- **IPFS pinning**: Pins all content to IPFS cluster

## Output

The script will:
- Clone all repositories to `GIT_REPOS_DIR`
- Download all release assets to `ATTACHMENT_DIR`
- Pin all content to IPFS cluster
- Generate progress reports

## Error Handling

- Failed repositories and releases are tracked in progress file
- Script can be restarted to retry failed items
- Detailed error logging for troubleshooting

## Build

```bash
go mod download
go build -o clone ./cmd/clone
```
