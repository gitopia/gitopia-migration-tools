# Gitopia Migration Tools

This project contains three separate components for migrating Gitopia repositories, LFS objects, and release assets during the v6 upgrade process.

## Components Overview

### 1. Clone Script (`clone-script/`)
- **Purpose**: Clones all existing repositories (including LFS objects) and release assets
- **Version**: gitopia v5.1.0, gitopia-go v0.6.2
- **Usage**: Run before upgrade height to download and pin all existing data

### 2. Sync Daemon (`sync-daemon/`)
- **Purpose**: Syncs new changes from git-server until switchover to storage provider
- **Version**: gitopia v5.1.0, gitopia-go v0.6.2
- **Usage**: Run continuously before upgrade height to keep data synchronized

### 3. Update Script (`update-script/`)
- **Purpose**: Updates packfile, LFS objects and release assets transactions after chain upgrade
- **Version**: gitopia v6.0.0-rc.6, gitopia-go v0.7.0-rc.3
- **Usage**: Run after upgrade height to update blockchain with new storage information

## Migration Process

### Before Upgrade Height
1. Run **Clone Script** to download all existing data
2. Start **Sync Daemon** to keep data synchronized with git-server updates

### After Upgrade Height
1. Stop git-server
2. Register as storage provider
3. Run **Update Script** to update blockchain with storage information

## Directory Structure

```
gitopia-migration-tools/
├── README.md
├── clone-script/
│   ├── cmd/
│   │   └── clone/
│   │       └── main.go
│   ├── go.mod
│   ├── go.sum
│   ├── config.toml.example
│   └── README.md
├── sync-daemon/
│   ├── cmd/
│   │   └── syncd/
│   │       └── main.go
│   ├── handler/
│   ├── go.mod
│   ├── go.sum
│   ├── config.toml.example
│   └── README.md
└── update-script/
    ├── cmd/
    │   └── update/
    │       └── main.go
    ├── go.mod
    ├── go.sum
    ├── config.toml.example
    └── README.md
```

## Security Considerations

- Each component uses different versions of gitopia and gitopia-go
- IPFS cluster secret must be shared securely between all storage providers
- Wallet mnemonics should be stored securely and never committed to version control
