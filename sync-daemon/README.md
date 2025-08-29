# Sync Daemon

This component continuously syncs new changes from git-server until switchover to storage provider during the v6 upgrade process.

## Purpose

- Monitors blockchain events for repository and release changes
- Automatically downloads and pins new content to IPFS cluster
- Keeps storage provider synchronized with git-server updates
- Uses Gitopia v5.1.0 and gitopia-go v0.6.2 for pre-upgrade compatibility

## Prerequisites

- IPFS Kubo v0.25+
- IPFS Cluster v1.0+
- git-remote-gitopia configured
- Access to Gitopia v5.1.0 blockchain
- WebSocket connection to Tendermint RPC

## Configuration

1. Copy the example configuration:
   ```bash
   cp config.toml.example config.toml
   ```

2. Edit `config.toml` with your settings:
   - `GIT_REPOS_DIR`: Directory for repository cache
   - `ATTACHMENT_DIR`: Directory for release assets
   - `GITOPIA_ADDR`: Gitopia gRPC endpoint
   - `TM_ADDR`: Tendermint RPC endpoint (must support WebSocket)
   - `IPFS_CLUSTER_PEER_HOST/PORT`: IPFS cluster connection
   - `GIT_SERVER_HOST`: Git server URL for downloading assets

## Usage

1. **Setup wallet key:**
   ```bash
   ./syncd keys add gitopia-storage --keyring-backend test --recover
   ```

2. **Run the sync daemon:**
   ```bash
   ./syncd --from gitopia-storage --keyring-backend test
   ```

## Monitored Events

The daemon subscribes to these blockchain events:

### Repository Events
- `MsgMultiSetBranch`: Branch updates
- `MsgMultiSetTag`: Tag updates

### Release Events
- `CreateRelease`: New releases
- `UpdateRelease`: Release modifications
- `DeleteRelease`: Release deletions
- `DaoCreateRelease`: DAO-created releases

## Features

- **Real-time sync**: WebSocket-based event monitoring
- **Automatic cloning**: Downloads new repositories as needed
- **Incremental updates**: Fetches only changed content
- **LFS support**: Handles Git LFS objects
- **Asset management**: Downloads and pins release assets
- **Error resilience**: Continues processing despite individual failures

## Event Processing

### Branch/Tag Updates
1. Fetches repository changes
2. Runs `git gc` to optimize packfiles
3. Pins new packfiles to IPFS cluster
4. Processes any new LFS objects

### Release Events
1. Downloads new/modified release assets
2. Verifies SHA256 checksums
3. Pins assets to IPFS cluster
4. Handles asset deletions for release updates

## Build

```bash
go mod download
go build -o syncd ./cmd/syncd
```

## Stopping

The daemon runs continuously until stopped with `Ctrl+C` or system signal.
