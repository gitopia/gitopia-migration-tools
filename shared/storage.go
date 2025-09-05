package shared

import (
	"database/sql"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	_ "github.com/mattn/go-sqlite3"
	"github.com/pkg/errors"
)

// PackfileInfo stores information about a repository's packfile
type PackfileInfo struct {
	RepositoryID uint64    `json:"repository_id"`
	Name         string    `json:"name"`
	CID          string    `json:"cid"`
	RootHash     []byte    `json:"root_hash"`
	Size         int64     `json:"size"`
	UpdatedAt    time.Time `json:"updated_at"`
	UpdatedBy    string    `json:"updated_by"` // "clone" or "sync"
}

// LFSObjectInfo stores information about an LFS object
type LFSObjectInfo struct {
	RepositoryID uint64    `json:"repository_id"`
	OID          string    `json:"oid"`
	CID          string    `json:"cid"`
	RootHash     []byte    `json:"root_hash"`
	Size         int64     `json:"size"`
	UpdatedAt    time.Time `json:"updated_at"`
	UpdatedBy    string    `json:"updated_by"` // "clone" or "sync"
}

// ReleaseAssetInfo stores information about a release asset
type ReleaseAssetInfo struct {
	RepositoryID uint64    `json:"repository_id"`
	TagName      string    `json:"tag_name"`
	Name         string    `json:"name"`
	CID          string    `json:"cid"`
	RootHash     []byte    `json:"root_hash"`
	Size         int64     `json:"size"`
	SHA256       string    `json:"sha256"`
	UpdatedAt    time.Time `json:"updated_at"`
	UpdatedBy    string    `json:"updated_by"` // "clone" or "sync"
}

// StorageManager manages persistent storage of migration data using SQLite
type StorageManager struct {
	mu     sync.RWMutex
	db     *sql.DB
	dbPath string
}

// NewStorageManager creates a new storage manager with SQLite backend
func NewStorageManager(workingDir string) *StorageManager {
	dbPath := filepath.Join(workingDir, "migration_data.db")
	return &StorageManager{
		dbPath: dbPath,
	}
}

// Load initializes the SQLite database and creates tables if they don't exist
func (sm *StorageManager) Load() error {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	// Create working directory if it doesn't exist
	workingDir := filepath.Dir(sm.dbPath)
	if err := os.MkdirAll(workingDir, 0755); err != nil {
		return errors.Wrap(err, "failed to create working directory")
	}

	// Open SQLite database
	db, err := sql.Open("sqlite3", sm.dbPath+"?_journal_mode=WAL&_synchronous=NORMAL&_cache_size=1000")
	if err != nil {
		return errors.Wrap(err, "failed to open database")
	}
	sm.db = db

	// Create tables
	if err := sm.createTables(); err != nil {
		return errors.Wrap(err, "failed to create tables")
	}

	return nil
}

// createTables creates the necessary SQLite tables
func (sm *StorageManager) createTables() error {
	// Create packfiles table
	_, err := sm.db.Exec(`
		CREATE TABLE IF NOT EXISTS packfiles (
			repository_id INTEGER PRIMARY KEY,
			name TEXT NOT NULL,
			cid TEXT NOT NULL,
			root_hash TEXT NOT NULL,
			size INTEGER NOT NULL,
			updated_at DATETIME NOT NULL,
			updated_by TEXT NOT NULL
		)
	`)
	if err != nil {
		return errors.Wrap(err, "failed to create packfiles table")
	}

	// Create LFS objects table
	_, err = sm.db.Exec(`
		CREATE TABLE IF NOT EXISTS lfs_objects (
			repository_id INTEGER NOT NULL,
			oid TEXT NOT NULL,
			cid TEXT NOT NULL,
			root_hash TEXT NOT NULL,
			size INTEGER NOT NULL,
			updated_at DATETIME NOT NULL,
			updated_by TEXT NOT NULL,
			PRIMARY KEY (repository_id, oid)
		)
	`)
	if err != nil {
		return errors.Wrap(err, "failed to create lfs_objects table")
	}

	// Create release assets table
	_, err = sm.db.Exec(`
		CREATE TABLE IF NOT EXISTS release_assets (
			repository_id INTEGER NOT NULL,
			tag_name TEXT NOT NULL,
			name TEXT NOT NULL,
			cid TEXT NOT NULL,
			root_hash TEXT NOT NULL,
			size INTEGER NOT NULL,
			sha256 TEXT NOT NULL,
			updated_at DATETIME NOT NULL,
			updated_by TEXT NOT NULL,
			PRIMARY KEY (repository_id, tag_name, name)
		)
	`)
	if err != nil {
		return errors.Wrap(err, "failed to create release_assets table")
	}

	// Create indexes for better performance
	_, err = sm.db.Exec(`CREATE INDEX IF NOT EXISTS idx_packfiles_updated_by ON packfiles(updated_by)`)
	if err != nil {
		return errors.Wrap(err, "failed to create packfiles index")
	}

	_, err = sm.db.Exec(`CREATE INDEX IF NOT EXISTS idx_lfs_objects_updated_by ON lfs_objects(updated_by)`)
	if err != nil {
		return errors.Wrap(err, "failed to create lfs_objects index")
	}

	_, err = sm.db.Exec(`CREATE INDEX IF NOT EXISTS idx_release_assets_updated_by ON release_assets(updated_by)`)
	if err != nil {
		return errors.Wrap(err, "failed to create release_assets index")
	}

	return nil
}

// shouldUpdate determines if an update should be applied based on precedence rules
// sync daemon updates take precedence over clone script updates
func shouldUpdate(existingUpdatedBy string, existingUpdatedAt time.Time, newUpdatedBy string, newUpdatedAt time.Time) bool {
	// If existing is from sync daemon, only update if new is also from sync daemon and newer
	if existingUpdatedBy == "sync" {
		return newUpdatedBy == "sync" && newUpdatedAt.After(existingUpdatedAt)
	}
	// If existing is from clone script, always update if new is from sync daemon
	// or if new is from clone script and newer
	if newUpdatedBy == "sync" {
		return true
	}
	return newUpdatedAt.After(existingUpdatedAt)
}

// Save is now a no-op since SQLite handles persistence automatically
func (sm *StorageManager) Save() error {
	// SQLite handles persistence automatically, no need to explicitly save
	return nil
}

// SetPackfileInfo stores packfile information with precedence handling
func (sm *StorageManager) SetPackfileInfo(info *PackfileInfo) {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	// Check if we should update based on precedence
	var existing *PackfileInfo
	row := sm.db.QueryRow("SELECT name, cid, root_hash, size, updated_at, updated_by FROM packfiles WHERE repository_id = ?", info.RepositoryID)
	var name, cid, rootHashHex, updatedBy string
	var size int64
	var updatedAt time.Time
	if err := row.Scan(&name, &cid, &rootHashHex, &size, &updatedAt, &updatedBy); err == nil {
		rootHash, _ := hex.DecodeString(rootHashHex)
		existing = &PackfileInfo{
			RepositoryID: info.RepositoryID,
			Name:         name,
			CID:          cid,
			RootHash:     rootHash,
			Size:         size,
			UpdatedAt:    updatedAt,
			UpdatedBy:    updatedBy,
		}
	}

	if existing == nil || shouldUpdate(existing.UpdatedBy, existing.UpdatedAt, info.UpdatedBy, info.UpdatedAt) {
		_, err := sm.db.Exec(`
			INSERT OR REPLACE INTO packfiles 
			(repository_id, name, cid, root_hash, size, updated_at, updated_by) 
			VALUES (?, ?, ?, ?, ?, ?, ?)
		`, info.RepositoryID, info.Name, info.CID, hex.EncodeToString(info.RootHash), info.Size, info.UpdatedAt, info.UpdatedBy)
		if err != nil {
			// Log error but don't fail the operation
			fmt.Printf("Error storing packfile info: %v\n", err)
		}
	}
}

// GetPackfileInfo retrieves packfile information
func (sm *StorageManager) GetPackfileInfo(repositoryID uint64) *PackfileInfo {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	row := sm.db.QueryRow("SELECT name, cid, root_hash, size, updated_at, updated_by FROM packfiles WHERE repository_id = ?", repositoryID)
	var name, cid, rootHashHex, updatedBy string
	var size int64
	var updatedAt time.Time
	if err := row.Scan(&name, &cid, &rootHashHex, &size, &updatedAt, &updatedBy); err != nil {
		return nil
	}

	// Convert hex string back to bytes
	rootHash, err := hex.DecodeString(rootHashHex)
	if err != nil {
		return nil
	}

	return &PackfileInfo{
		RepositoryID: repositoryID,
		Name:         name,
		CID:          cid,
		RootHash:     rootHash,
		Size:         size,
		UpdatedAt:    updatedAt,
		UpdatedBy:    updatedBy,
	}
}

// SetLFSObjectInfo sets LFS object information (sync daemon takes precedence)
func (sm *StorageManager) SetLFSObjectInfo(info *LFSObjectInfo) {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	// Check if we should update based on precedence
	var existing *LFSObjectInfo
	row := sm.db.QueryRow("SELECT cid, root_hash, size, updated_at, updated_by FROM lfs_objects WHERE repository_id = ? AND oid = ?", info.RepositoryID, info.OID)
	var cid, rootHashHex, updatedBy string
	var size int64
	var updatedAt time.Time
	if err := row.Scan(&cid, &rootHashHex, &size, &updatedAt, &updatedBy); err == nil {
		rootHash, _ := hex.DecodeString(rootHashHex)
		existing = &LFSObjectInfo{
			RepositoryID: info.RepositoryID,
			OID:          info.OID,
			CID:          cid,
			RootHash:     rootHash,
			Size:         size,
			UpdatedAt:    updatedAt,
			UpdatedBy:    updatedBy,
		}
	}

	if existing == nil || shouldUpdate(existing.UpdatedBy, existing.UpdatedAt, info.UpdatedBy, info.UpdatedAt) {
		_, err := sm.db.Exec(`
			INSERT OR REPLACE INTO lfs_objects 
			(repository_id, oid, cid, root_hash, size, updated_at, updated_by) 
			VALUES (?, ?, ?, ?, ?, ?, ?)
		`, info.RepositoryID, info.OID, info.CID, hex.EncodeToString(info.RootHash), info.Size, info.UpdatedAt, info.UpdatedBy)
		if err != nil {
			// Log error but don't fail the operation
			fmt.Printf("Error storing LFS object info: %v\n", err)
		}
	}
}

// GetLFSObjectInfo retrieves LFS object information
func (sm *StorageManager) GetLFSObjectInfo(repositoryID uint64, oid string) *LFSObjectInfo {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	row := sm.db.QueryRow("SELECT cid, root_hash, size, updated_at, updated_by FROM lfs_objects WHERE repository_id = ? AND oid = ?", repositoryID, oid)
	var cid, rootHashHex, updatedBy string
	var size int64
	var updatedAt time.Time
	if err := row.Scan(&cid, &rootHashHex, &size, &updatedAt, &updatedBy); err != nil {
		return nil
	}

	// Convert hex string back to bytes
	rootHash, err := hex.DecodeString(rootHashHex)
	if err != nil {
		return nil
	}

	return &LFSObjectInfo{
		RepositoryID: repositoryID,
		OID:          oid,
		CID:          cid,
		RootHash:     rootHash,
		Size:         size,
		UpdatedAt:    updatedAt,
		UpdatedBy:    updatedBy,
	}
}

// SetReleaseAssetInfo sets release asset information (sync daemon takes precedence)
func (sm *StorageManager) SetReleaseAssetInfo(info *ReleaseAssetInfo) {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	// Check if we should update based on precedence
	var existing *ReleaseAssetInfo
	row := sm.db.QueryRow("SELECT cid, root_hash, size, sha256, updated_at, updated_by FROM release_assets WHERE repository_id = ? AND tag_name = ? AND name = ?", info.RepositoryID, info.TagName, info.Name)
	var cid, rootHashHex, sha256, updatedBy string
	var size int64
	var updatedAt time.Time
	if err := row.Scan(&cid, &rootHashHex, &size, &sha256, &updatedAt, &updatedBy); err == nil {
		rootHash, _ := hex.DecodeString(rootHashHex)
		existing = &ReleaseAssetInfo{
			RepositoryID: info.RepositoryID,
			TagName:      info.TagName,
			Name:         info.Name,
			CID:          cid,
			RootHash:     rootHash,
			Size:         size,
			SHA256:       sha256,
			UpdatedAt:    updatedAt,
			UpdatedBy:    updatedBy,
		}
	}

	if existing == nil || shouldUpdate(existing.UpdatedBy, existing.UpdatedAt, info.UpdatedBy, info.UpdatedAt) {
		_, err := sm.db.Exec(`
			INSERT OR REPLACE INTO release_assets 
			(repository_id, tag_name, name, cid, root_hash, size, sha256, updated_at, updated_by) 
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
		`, info.RepositoryID, info.TagName, info.Name, info.CID, hex.EncodeToString(info.RootHash), info.Size, info.SHA256, info.UpdatedAt, info.UpdatedBy)
		if err != nil {
			// Log error but don't fail the operation
			fmt.Printf("Error storing release asset info: %v\n", err)
		}
	}
}

// GetReleaseAssetInfo retrieves release asset information
func (sm *StorageManager) GetReleaseAssetInfo(repositoryID uint64, tagName, assetName string) *ReleaseAssetInfo {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	row := sm.db.QueryRow("SELECT cid, root_hash, size, sha256, updated_at, updated_by FROM release_assets WHERE repository_id = ? AND tag_name = ? AND name = ?", repositoryID, tagName, assetName)
	var cid, rootHashHex, sha256, updatedBy string
	var size int64
	var updatedAt time.Time
	if err := row.Scan(&cid, &rootHashHex, &size, &sha256, &updatedAt, &updatedBy); err != nil {
		return nil
	}

	// Convert hex string back to bytes
	rootHash, err := hex.DecodeString(rootHashHex)
	if err != nil {
		return nil
	}

	return &ReleaseAssetInfo{
		RepositoryID: repositoryID,
		TagName:      tagName,
		Name:         assetName,
		CID:          cid,
		RootHash:     rootHash,
		Size:         size,
		SHA256:       sha256,
		UpdatedAt:    updatedAt,
		UpdatedBy:    updatedBy,
	}
}

// GetAllPackfileInfo returns all packfile information
func (sm *StorageManager) GetAllPackfileInfo() []*PackfileInfo {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	rows, err := sm.db.Query("SELECT repository_id, name, cid, root_hash, size, updated_at, updated_by FROM packfiles")
	if err != nil {
		return nil
	}
	defer rows.Close()

	var result []*PackfileInfo
	for rows.Next() {
		var repositoryID uint64
		var name, cid, rootHashHex, updatedBy string
		var size int64
		var updatedAt time.Time
		if err := rows.Scan(&repositoryID, &name, &cid, &rootHashHex, &size, &updatedAt, &updatedBy); err != nil {
			continue
		}

		// Convert hex string back to bytes
		rootHash, err := hex.DecodeString(rootHashHex)
		if err != nil {
			continue
		}

		result = append(result, &PackfileInfo{
			RepositoryID: repositoryID,
			Name:         name,
			CID:          cid,
			RootHash:     rootHash,
			Size:         size,
			UpdatedAt:    updatedAt,
			UpdatedBy:    updatedBy,
		})
	}
	return result
}

// GetAllLFSObjects returns all LFS object information
func (sm *StorageManager) GetAllLFSObjects() []*LFSObjectInfo {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	rows, err := sm.db.Query("SELECT repository_id, oid, cid, root_hash, size, updated_at, updated_by FROM lfs_objects")
	if err != nil {
		return nil
	}
	defer rows.Close()

	var result []*LFSObjectInfo
	for rows.Next() {
		var repositoryID uint64
		var oid, cid, rootHashHex, updatedBy string
		var size int64
		var updatedAt time.Time
		if err := rows.Scan(&repositoryID, &oid, &cid, &rootHashHex, &size, &updatedAt, &updatedBy); err != nil {
			continue
		}

		// Convert hex string back to bytes
		rootHash, err := hex.DecodeString(rootHashHex)
		if err != nil {
			continue
		}

		result = append(result, &LFSObjectInfo{
			RepositoryID: repositoryID,
			OID:          oid,
			CID:          cid,
			RootHash:     rootHash,
			Size:         size,
			UpdatedAt:    updatedAt,
			UpdatedBy:    updatedBy,
		})
	}
	return result
}

// GetAllReleaseAssets returns all release asset information
func (sm *StorageManager) GetAllReleaseAssets() map[string]*ReleaseAssetInfo {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	result := make(map[string]*ReleaseAssetInfo)
	rows, err := sm.db.Query("SELECT repository_id, tag_name, name, cid, root_hash, size, sha256, updated_at, updated_by FROM release_assets")
	if err != nil {
		return nil
	}
	defer rows.Close()

	for rows.Next() {
		var repositoryID uint64
		var tagName, name, cid, rootHashHex, sha256, updatedBy string
		var size int64
		var updatedAt time.Time
		if err := rows.Scan(&repositoryID, &tagName, &name, &cid, &rootHashHex, &size, &sha256, &updatedAt, &updatedBy); err != nil {
			continue
		}

		// Convert hex string back to bytes
		rootHash, err := hex.DecodeString(rootHashHex)
		if err != nil {
			continue
		}

		result[fmt.Sprintf("%d:%s:%s", repositoryID, tagName, name)] = &ReleaseAssetInfo{
			RepositoryID: repositoryID,
			TagName:      tagName,
			Name:         name,
			CID:          cid,
			RootHash:     rootHash,
			Size:         size,
			SHA256:       sha256,
			UpdatedAt:    updatedAt,
			UpdatedBy:    updatedBy,
		}
	}
	return result
}

// GetLFSObjectsByRepository returns all LFS objects for a specific repository
func (sm *StorageManager) GetLFSObjectsByRepository(repositoryID uint64) []*LFSObjectInfo {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	rows, err := sm.db.Query("SELECT oid, cid, root_hash, size, updated_at, updated_by FROM lfs_objects WHERE repository_id = ?", repositoryID)
	if err != nil {
		return nil
	}
	defer rows.Close()

	var result []*LFSObjectInfo
	for rows.Next() {
		var oid, cid, rootHashHex, updatedBy string
		var size int64
		var updatedAt time.Time
		if err := rows.Scan(&oid, &cid, &rootHashHex, &size, &updatedAt, &updatedBy); err != nil {
			continue
		}

		// Convert hex string back to bytes
		rootHash, err := hex.DecodeString(rootHashHex)
		if err != nil {
			continue
		}

		result = append(result, &LFSObjectInfo{
			RepositoryID: repositoryID,
			OID:          oid,
			CID:          cid,
			RootHash:     rootHash,
			Size:         size,
			UpdatedAt:    updatedAt,
			UpdatedBy:    updatedBy,
		})
	}
	return result
}

// GetReleaseAssetsByRepository returns all release assets for a specific repository
func (sm *StorageManager) GetReleaseAssetsByRepository(repositoryID uint64) []*ReleaseAssetInfo {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	rows, err := sm.db.Query("SELECT tag_name, name, cid, root_hash, size, sha256, updated_at, updated_by FROM release_assets WHERE repository_id = ?", repositoryID)
	if err != nil {
		return nil
	}
	defer rows.Close()

	var result []*ReleaseAssetInfo
	for rows.Next() {
		var tagName, name, cid, rootHashHex, sha256, updatedBy string
		var size int64
		var updatedAt time.Time
		if err := rows.Scan(&tagName, &name, &cid, &rootHashHex, &size, &sha256, &updatedAt, &updatedBy); err != nil {
			continue
		}

		// Convert hex string back to bytes
		rootHash, err := hex.DecodeString(rootHashHex)
		if err != nil {
			continue
		}

		result = append(result, &ReleaseAssetInfo{
			RepositoryID: repositoryID,
			TagName:      tagName,
			Name:         name,
			CID:          cid,
			RootHash:     rootHash,
			Size:         size,
			SHA256:       sha256,
			UpdatedAt:    updatedAt,
			UpdatedBy:    updatedBy,
		})
	}
	return result
}

// GetReleaseAssetsByRepositoryAndTag returns all release assets for a specific repository and tag
func (sm *StorageManager) GetReleaseAssetsByRepositoryAndTag(repositoryID uint64, tagName string) []*ReleaseAssetInfo {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	rows, err := sm.db.Query("SELECT name, cid, root_hash, size, sha256, updated_at, updated_by FROM release_assets WHERE repository_id = ? AND tag_name = ?", repositoryID, tagName)
	if err != nil {
		return nil
	}
	defer rows.Close()

	var result []*ReleaseAssetInfo
	for rows.Next() {
		var name, cid, rootHashHex, sha256, updatedBy string
		var size int64
		var updatedAt time.Time
		if err := rows.Scan(&name, &cid, &rootHashHex, &size, &sha256, &updatedAt, &updatedBy); err != nil {
			continue
		}

		// Convert hex string back to bytes
		rootHash, err := hex.DecodeString(rootHashHex)
		if err != nil {
			continue
		}

		result = append(result, &ReleaseAssetInfo{
			RepositoryID: repositoryID,
			TagName:      tagName,
			Name:         name,
			CID:          cid,
			RootHash:     rootHash,
			Size:         size,
			SHA256:       sha256,
			UpdatedAt:    updatedAt,
			UpdatedBy:    updatedBy,
		})
	}
	return result
}

// Close closes the database connection
func (sm *StorageManager) Close() error {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	if sm.db != nil {
		return sm.db.Close()
	}
	return nil
}
