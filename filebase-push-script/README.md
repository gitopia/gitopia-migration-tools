# Filebase Push Script

This script queries packfiles, release assets, and LFS objects from the Gitopia chain, downloads them from a local IPFS server, and uploads them to Filebase storage.

## Features

- **Query with Pagination**: Automatically handles pagination when querying large datasets from the Gitopia chain
- **Progress Tracking**: Keeps track of uploaded files to avoid duplicates and support resuming from failures
- **Resume Support**: Can resume from where it left off if the process is interrupted
- **Selective Processing**: Options to process only specific types of objects (packfiles, release assets, or LFS objects)
- **Automatic Cleanup**: Removes temporary files after successful upload
- **Error Handling**: Continues processing even if individual files fail

## Prerequisites

1. Local IPFS server running and accessible
2. Filebase account with S3 credentials
3. Access to Gitopia chain for querying

## Configuration

1. Copy the example configuration:
   ```bash
   cp config.toml.example config.toml
   ```

2. Update `config.toml` with your settings:
   - **Gitopia Network**: Chain endpoints and configuration
   - **IPFS**: Local IPFS server host and port
   - **Filebase**: S3 credentials and bucket information

## Usage

### Build the script
```bash
go build -o filebase-push ./cmd/push
```

### Run the script

#### Push all types of objects:
```bash
./filebase-push --from your_key_name
```

#### Push only packfiles:
```bash
./filebase-push --packfiles-only --from your_key_name
```

#### Push only release assets:
```bash
./filebase-push --assets-only --from your_key_name
```

#### Push only LFS objects:
```bash
./filebase-push --lfs-only --from your_key_name
```

#### Resume from a specific progress file:
```bash
./filebase-push --progress-file custom-progress.json --from your_key_name
```

## Command Line Options

- `--from`: Name or address of private key for signing (required)
- `--keyring-backend`: Keyring backend (os|file|kwallet|pass|test|memory)
- `--packfiles-only`: Process only packfiles
- `--assets-only`: Process only release assets  
- `--lfs-only`: Process only LFS objects
- `--progress-file`: File to store upload progress (default: filebase-push-progress.json)

## How It Works

1. **Query Phase**: The script queries the Gitopia chain for all packfiles, release assets, and LFS objects using paginated requests
2. **Download Phase**: For each object, it downloads the file from the local IPFS server using the stored CID
3. **Upload Phase**: The downloaded file is uploaded to Filebase using the S3 API
4. **Tracking Phase**: Progress is saved to a JSON file to enable resuming and avoid duplicates
5. **Cleanup Phase**: Temporary files are removed after successful upload

## File Organization in Filebase

Files are organized in the following structure:
- **Packfiles**: `packfiles/{packfile_name}`
- **Release Assets**: `release-assets/{repository_id}/{tag}/{asset_name}`
- **LFS Objects**: `lfs-objects/{repository_id}/{oid}`

## Progress Tracking

The script maintains a progress file (JSON format) that tracks:
- CID of the uploaded file
- Original name/identifier
- File type (packfile, release_asset, lfs_object)
- File size
- Upload timestamp

This allows the script to:
- Skip files that have already been uploaded
- Resume from interruptions
- Provide visibility into what has been processed

## Error Handling

- Individual file failures don't stop the entire process
- Failed downloads/uploads are logged and skipped
- Temporary files are cleaned up even on failures
- Progress is saved incrementally to minimize data loss

## Troubleshooting

### Common Issues

1. **IPFS Connection Failed**: Ensure your local IPFS server is running and accessible
2. **Filebase Upload Failed**: Check your S3 credentials and bucket permissions
3. **Chain Query Failed**: Verify Gitopia chain endpoints are accessible
4. **Permission Denied**: Ensure the script has write permissions for temp directory and progress file

### Logs

The script provides detailed logging including:
- Progress indicators for each file processed
- Success/failure status for each operation
- Error messages for debugging

### Recovery

If the script fails:
1. Check the progress file to see what was completed
2. Fix any configuration issues
3. Re-run the script - it will automatically resume from where it left off
