# Gitopia Repository Verification Script

This script verifies that all repositories have been successfully stored in IPFS and validates that all refs (branches and tags) on-chain exist in the packfiles fetched from IPFS.

## Features

- **Complete Repository Verification**: Iterates through all repositories from ID 0 to maximum repository ID
- **IPFS Packfile Validation**: Downloads packfiles from IPFS and verifies all Git objects exist
- **Fork Repository Support**: Handles forked repositories using Git alternates for efficient verification
- **Efficient Large File Handling**: Streams large packfiles (>1GB) from IPFS to avoid memory issues
- **Non-blocking Error Handling**: Logs verification failures without stopping the entire process
- **Comprehensive Logging**: Tracks all verification failures with detailed information

## Configuration

Copy `config.toml.example` to `config.toml` and update the values:

```toml
# Working directory (must match clone script for shared database access)
WORKING_DIR = "/var/gitopia-migration"

# Gitopia Network Configuration
GITOPIA_ADDR = "gitopia-grpc.polkachu.com:11390"
TM_ADDR = "https://gitopia-rpc.polkachu.com:443"
CHAIN_ID = "gitopia"
GAS_PRICES = "0.001ulore"

# IPFS Configuration
IPFS_HOST = "127.0.0.1"
IPFS_PORT = "5001"
```

## Usage

```bash
# Build the verification script
go build -o verify ./cmd/verify

# Run verification
./verify
```

## How It Works

1. **Database Access**: Uses the same SQLite database as the clone script to get packfile information
2. **Repository Iteration**: Processes repositories sequentially from ID 0 to maximum ID
3. **Fork Handling**: For forked repositories:
   - Downloads parent repository packfile first
   - Sets up Git alternates to reference parent objects
   - Only verifies fork-specific objects
4. **IPFS Streaming**: Downloads packfiles using IPFS HTTP API with streaming for memory efficiency
5. **Git Verification**: Uses `git cat-file -e` to verify each branch and tag SHA exists in the packfile
6. **Error Logging**: Records failures in `verification_failures.json` with detailed information

## Output

The script generates a `verification_failures.json` file containing any verification failures:

```json
{
  "failures": [
    {
      "repository_id": 12345,
      "ref_name": "refs/heads/main",
      "ref_sha": "abc123...",
      "error": "object not found in packfile",
      "timestamp": "2024-01-01T12:00:00Z"
    }
  ]
}
```

## Performance Considerations

- **Memory Efficient**: Streams large packfiles instead of loading into memory
- **Skip Large Repositories**: Automatically skips repository 62771 (30GB binary files)
- **Periodic Logging**: Saves verification log every 1000 repositories
- **Minimal Output**: Reduces log verbosity for processing thousands of repositories

## Special Cases

- **Empty Repositories**: Skipped if no default branch exists
- **Missing Packfiles**: Logged as failures if not found in database
- **Fork Dependencies**: Parent repositories are automatically processed when needed
- **Large Files**: Uses 30-minute timeout for downloading large packfiles from IPFS

## Error Recovery

The script is designed to be resilient:
- Individual repository failures don't stop the entire process
- All failures are logged with detailed error information
- Can be safely rerun (will re-verify all repositories)
- Uses the same database as clone script for consistency
