//go:build integration

package integration

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/hashmap-kz/storecrypt/pkg/storage"
	"github.com/hashmap-kz/streamcrypt/pkg/codec"
	"github.com/hashmap-kz/streamcrypt/pkg/crypt/aesgcm"
	"github.com/stretchr/testify/require"
)

// initDynStoragesSameBackendT creates a single Local backend and several
// VariadicStorage wrappers with different writeExts, all sharing that backend.
func initDynStoragesSameBackendT(t *testing.T, subpath string) map[string]*storage.VariadicStorage {
	t.Helper()

	baseDir := t.TempDir()
	local, err := storage.NewLocal(&storage.LocalStorageOpts{
		BaseDir:      filepath.Join(baseDir, subpath),
		FsyncOnWrite: false,
	})
	require.NoError(t, err)

	alg := storage.Algorithms{
		Gzip: &storage.CodecPair{
			Compressor:   codec.GzipCompressor{},
			Decompressor: codec.GzipDecompressor{},
		},
		Zstd: &storage.CodecPair{
			Compressor:   codec.ZstdCompressor{},
			Decompressor: codec.ZstdDecompressor{},
		},
		AES: aesgcm.NewChunkedGCMCrypter("password"),
	}

	newVS := func(writeExt string) *storage.VariadicStorage {
		st, err := storage.NewVariadicStorage(local, alg, writeExt)
		require.NoError(t, err, "NewVariadicStorage(%q) failed", writeExt)
		return st
	}

	return map[string]*storage.VariadicStorage{
		"plain":   newVS(""),
		"gz":      newVS(".gz"),
		"zst":     newVS(".zst"),
		"aes":     newVS(".aes"),
		"gz.aes":  newVS(".gz.aes"),
		"zst.aes": newVS(".zst.aes"),
	}
}

func TestDynStorage_CrossReadWrite_AllVariants(t *testing.T) {
	ctx := context.TODO()
	storages := initDynStoragesSameBackendT(t, t.Name())

	logicalPath := "wal/000000010000000000000001"
	content := []byte("hello wal world")

	// We only need a single backend for cleanup, pick any.
	var backend storage.Storage
	for _, s := range storages {
		backend = s.Backend
		break
	}
	require.NotNil(t, backend)

	for wName, writer := range storages {
		for rName, reader := range storages {
			t.Run(fmt.Sprintf("write=%s/read=%s", wName, rName), func(t *testing.T) {
				// Clean backend before each pair
				require.NoError(t, backend.DeleteAll(ctx, ""), "cleanup before pair")

				// Write through writer
				err := writer.Put(ctx, logicalPath, bytes.NewReader(content))
				require.NoError(t, err, "[write=%s] Put failed", wName)

				// Read through reader using logical name
				exists, err := reader.Exists(ctx, logicalPath)
				require.NoError(t, err)
				assert.True(t, exists, "Exists should be true for logical path")

				rc, err := reader.Get(ctx, logicalPath)
				require.NoError(t, err, "[read=%s] Get failed", rName)
				got, err := io.ReadAll(rc)
				require.NoError(t, err)
				require.NoError(t, rc.Close())

				assert.Equal(t, content, got,
					"content mismatch write=%s read=%s", wName, rName)
			})
		}
	}
}

func TestDynStorage_Get_UsesHighestPriorityVariant(t *testing.T) {
	ctx := context.TODO()
	storages := initDynStoragesSameBackendT(t, t.Name())

	// We'll use the "plain" instance to call Get/Exists.
	stPlain := storages["plain"]
	require.NotNil(t, stPlain)

	backend := stPlain.Backend
	require.NoError(t, backend.DeleteAll(ctx, ""), "initial cleanup")

	const logical = "wal/000000010000000000000002"

	plainContent := []byte("plain-content")
	gzContent := []byte("gzip-content")
	gzAesContent := []byte("gzip-aes-content")

	// Write plain via "plain" storage
	require.NoError(t, storages["plain"].Put(ctx, logical, bytes.NewReader(plainContent)))
	// Write gzip via "gz" storage
	require.NoError(t, storages["gz"].Put(ctx, logical, bytes.NewReader(gzContent)))
	// Write gzip+aes via "gz.aes" storage
	require.NoError(t, storages["gz.aes"].Put(ctx, logical, bytes.NewReader(gzAesContent)))

	// Now a plain storage Get(logical) should pick the highest-priority variant
	// according to supportedExts() -> ".gz.aes" should win.
	rc, err := stPlain.Get(ctx, logical)
	require.NoError(t, err)
	got, err := io.ReadAll(rc)
	require.NoError(t, err)
	require.NoError(t, rc.Close())

	assert.Equal(t, gzAesContent, got, "Get should return content from .gz.aes (highest priority)")
}

func TestDynStorage_Delete_RemovesAllVariantsIntegration(t *testing.T) {
	ctx := context.TODO()
	storages := initDynStoragesSameBackendT(t, t.Name())

	stPlain := storages["plain"]
	require.NotNil(t, stPlain)
	backend := stPlain.Backend

	require.NoError(t, backend.DeleteAll(ctx, ""), "initial cleanup")

	const logical = "wal/000000010000000000000003"
	content := []byte("payload")

	// Create several variants for the same logical name.
	require.NoError(t, storages["plain"].Put(ctx, logical, bytes.NewReader(content)))
	require.NoError(t, storages["gz"].Put(ctx, logical, bytes.NewReader(content)))
	require.NoError(t, storages["aes"].Put(ctx, logical, bytes.NewReader(content)))
	require.NoError(t, storages["gz.aes"].Put(ctx, logical, bytes.NewReader(content)))

	// Sanity: backend should see all encoded paths as existing.
	for _, extName := range []string{logical, logical + ".gz", logical + ".aes", logical + ".gz.aes"} {
		exists, err := backend.Exists(ctx, filepath.ToSlash(extName))
		require.NoError(t, err)
		assert.True(t, exists, "backend should see %q before delete", extName)
	}

	// Now delete via VariadicStorage logical Delete.
	err := stPlain.Delete(ctx, logical)
	require.NoError(t, err)

	// Logical Exists should be false
	exists, err := stPlain.Exists(ctx, logical)
	require.NoError(t, err)
	assert.False(t, exists, "logical Exists should be false after Delete")

	// All encoded variants must be gone
	for _, extName := range []string{logical, logical + ".gz", logical + ".aes", logical + ".gz.aes"} {
		ex, err := backend.Exists(ctx, filepath.ToSlash(extName))
		require.NoError(t, err)
		assert.False(t, ex, "backend should not see %q after delete", extName)
	}
}

func TestDynStorage_Get_WithExplicitExtension(t *testing.T) {
	ctx := context.TODO()
	storages := initDynStoragesSameBackendT(t, t.Name())

	stGzAes := storages["gz.aes"]
	require.NotNil(t, stGzAes)
	backend := stGzAes.Backend

	require.NoError(t, backend.DeleteAll(ctx, ""), "initial cleanup")

	const logical = "wal/000000010000000000000004"
	content := []byte("explicit gz.aes")

	// Write via gz.aes wrapper using logical name.
	require.NoError(t, stGzAes.Put(ctx, logical, bytes.NewReader(content)))

	// Build explicit path with extension
	fullPath := logical + ".gz.aes"

	// Get by explicit ext through ANY instance – let's use "plain" here.
	stPlain := storages["plain"]

	rc, err := stPlain.Get(ctx, fullPath)
	require.NoError(t, err)
	got, err := io.ReadAll(rc)
	require.NoError(t, err)
	require.NoError(t, rc.Close())

	assert.Equal(t, content, got, "Get(fullPath) should decode gz.aes correctly")
}
