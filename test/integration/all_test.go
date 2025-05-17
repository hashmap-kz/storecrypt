//go:build integration

package integration

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log"
	"path/filepath"
	"testing"

	"github.com/hashmap-kz/storecrypt/pkg/storage"
	"github.com/hashmap-kz/streamcrypt/pkg/codec"
	"github.com/hashmap-kz/streamcrypt/pkg/crypt/aesgcm"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func impls(dir, subpath string) map[string]storage.Storage {
	localStorage, err := storage.NewLocal(&storage.LocalStorageOpts{
		BaseDir:      filepath.Join(dir, subpath),
		FsyncOnWrite: false,
	})
	if err != nil {
		log.Fatal(err)
	}

	return map[string]storage.Storage{
		"local": &storage.TransformingStorage{
			Backend:      localStorage,
			Crypter:      aesgcm.NewChunkedGCMCrypter("password"),
			Compressor:   codec.GzipCompressor{},
			Decompressor: codec.GzipDecompressor{},
		},
		"s3": &storage.TransformingStorage{
			Backend:      storage.NewS3Storage(createS3Client(), "backups", subpath),
			Crypter:      aesgcm.NewChunkedGCMCrypter("password"),
			Compressor:   codec.GzipCompressor{},
			Decompressor: codec.GzipDecompressor{},
		},
		"sftp": &storage.TransformingStorage{
			Backend:      storage.NewSFTPStorage(createSftpClient(), subpath),
			Crypter:      aesgcm.NewChunkedGCMCrypter("password"),
			Compressor:   codec.GzipCompressor{},
			Decompressor: codec.GzipDecompressor{},
		},
	}
}

func initStoragesT(t *testing.T, subpath string) map[string]storage.Storage {
	t.Helper()
	return impls(t.TempDir(), subpath)
}

func initStoragesB(b *testing.B, subpath string) map[string]storage.Storage {
	b.Helper()
	return impls(b.TempDir(), subpath)
}

func TestStorage_PutGetExists(t *testing.T) {
	ctx := context.TODO()
	storages := initStoragesT(t, t.Name())

	for name, store := range storages {
		t.Run(name, func(t *testing.T) {
			path := "folder/test.txt"
			content := []byte("hello secure world")

			err := store.Put(ctx, path, bytes.NewReader(content))
			require.NoError(t, err, "Put failed for storage: %s", name)

			exists, err := store.Exists(ctx, path)
			require.NoError(t, err, "Exists failed for storage: %s", name)
			assert.True(t, exists, "File should exist for storage: %s", name)

			r, err := store.Get(ctx, path)
			require.NoError(t, err, "Get failed for storage: %s", name)

			readContent, err := io.ReadAll(r)
			require.NoError(t, err)
			require.NoError(t, r.Close())
			assert.Equal(t, content, readContent, "Content mismatch for: %s", name)
		})
	}
}

func TestStorage_List(t *testing.T) {
	ctx := context.TODO()
	storages := initStoragesT(t, t.Name())

	for name, store := range storages {
		t.Run(name, func(t *testing.T) {
			files := []string{
				"list/a.txt",
				"list/b.txt",
				"list/c.txt",
			}

			for _, f := range files {
				err := store.Put(ctx, f, bytes.NewReader([]byte("x")))
				require.NoError(t, err, "[%s] Put failed for %s", name, f)
			}

			listed, err := store.List(ctx, "list")
			require.NoError(t, err, "[%s] List failed", name)
			assert.ElementsMatch(t, files, listed, "[%s] Listed files mismatch", name)
		})
	}
}

func TestStorage_Delete(t *testing.T) {
	ctx := context.TODO()
	storages := initStoragesT(t, t.Name())

	for name, store := range storages {
		t.Run(name, func(t *testing.T) {
			path := "del/test.txt"

			err := store.Put(ctx, path, bytes.NewReader([]byte("delete me")))
			require.NoError(t, err, "[%s] Put failed", name)

			err = store.Delete(ctx, path)
			require.NoError(t, err, "[%s] Delete failed", name)

			exists, err := store.Exists(ctx, path)
			require.NoError(t, err, "[%s] Exists check failed", name)
			assert.False(t, exists, "[%s] File should be deleted", name)
		})
	}
}

func TestStorage_DeleteAll(t *testing.T) {
	ctx := context.TODO()
	storages := initStoragesT(t, t.Name())

	for name, store := range storages {
		t.Run(name, func(t *testing.T) {
			files := []string{
				"bulk/f1.txt",
				"bulk/f2.txt",
				"bulk/f3.txt",
			}

			for _, f := range files {
				err := store.Put(ctx, f, bytes.NewReader([]byte("bulk content")))
				require.NoError(t, err, "[%s] Put failed for %s", name, f)
			}

			err := store.DeleteAll(ctx, "bulk")
			require.NoError(t, err, "[%s] DeleteAll failed", name)

			listed, err := store.List(ctx, "bulk")
			require.NoError(t, err, "[%s] List after DeleteAll failed", name)
			assert.Empty(t, listed, "[%s] Files not deleted by DeleteAll", name)
		})
	}
}

func TestStorage_PutObjectsWithPrefixes(t *testing.T) {
	ctx := context.TODO()
	storages := initStoragesT(t, t.Name())

	for name, r := range storages {
		t.Run(name, func(t *testing.T) {
			assert.NoError(t, r.DeleteAll(ctx, ""), "[%s] DeleteAll before put", name)

			// Upload 100 files with random nested prefixes
			for i := 0; i < 100; i++ {
				content := []byte("gzip + encrypted")
				prefix := genPaths(rnd(1, 14))
				logicalPath := fmt.Sprintf("%s/file-%d.txt", prefix, i)

				err := r.Put(ctx, logicalPath, bytes.NewReader(content))
				require.NoError(t, err, "[%s] Put failed for %s", name, logicalPath)
			}

			// List
			list, err := r.List(ctx, "")
			assert.NoError(t, err, "[%s] List failed", name)
			assert.Equal(t, 100, len(list), "[%s] Expected 100 files, got %d", name, len(list))

			// Cleanup
			require.NoError(t, r.DeleteAll(ctx, ""), "[%s] DeleteAll after test", name)

			// Confirm clean
			list, err = r.List(ctx, "")
			assert.NoError(t, err, "[%s] List after cleanup failed", name)
			assert.Equal(t, 0, len(list), "[%s] Files not fully cleaned up", name)
		})
	}
}

func TestStorage_HighLoad(t *testing.T) {
	ctx := context.TODO()
	storages := initStoragesT(t, t.Name())

	for name, store := range storages {
		t.Run(name, func(t *testing.T) {
			require.NoError(t, store.DeleteAll(ctx, ""), "[%s] DeleteAll before test", name)

			const fileCount = 100
			content := []byte("this is a stress test file content")

			// Upload
			for i := 0; i < fileCount; i++ {
				path := fmt.Sprintf("hl/%04d.txt", i)
				err := store.Put(ctx, path, bytes.NewReader(content))
				require.NoError(t, err, "[%s] Put failed for %s", name, path)
			}

			// List and check count
			list, err := store.List(ctx, "hl")
			require.NoError(t, err, "[%s] List failed", name)
			assert.Len(t, list, fileCount, "[%s] List returned wrong count", name)

			// Read and verify
			for i := 0; i < fileCount; i++ {
				path := fmt.Sprintf("hl/%04d.txt", i)

				exists, err := store.Exists(ctx, path)
				require.NoError(t, err, "[%s] Exists failed for %s", name, path)
				assert.True(t, exists, "[%s] File missing: %s", name, path)

				r, err := store.Get(ctx, path)
				require.NoError(t, err, "[%s] Get failed for %s", name, path)

				read, err := io.ReadAll(r)
				require.NoError(t, err)
				require.NoError(t, r.Close())
				assert.Equal(t, content, read, "[%s] Content mismatch for %s", name, path)
			}

			// Cleanup
			require.NoError(t, store.DeleteAll(ctx, "hl"), "[%s] DeleteAll cleanup failed", name)
			finalList, err := store.List(ctx, "hl")
			require.NoError(t, err)
			assert.Empty(t, finalList, "[%s] Files not cleaned up", name)
		})
	}
}

// go test -bench=. -benchmem -gcflags=-m github.com/hashmap-kz/xrepo/test/integration

func BenchmarkStorage_PutGet(b *testing.B) {
	ctx := context.TODO()
	storages := initStoragesB(b, b.Name())

	const (
		sampleCount = 100
		content     = "this is a stress test file content"
	)

	for name, store := range storages {
		b.Run(name, func(b *testing.B) {
			// Setup: upload sample files once
			for i := 0; i < sampleCount; i++ {
				path := fmt.Sprintf("bench/%04d.txt", i)
				err := store.Put(ctx, path, bytes.NewReader([]byte(content)))
				require.NoError(b, err, "[%s] Setup Put failed for %s", name, path)
			}

			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				idx := i % sampleCount
				path := fmt.Sprintf("bench/%04d.txt", idx)

				r, err := store.Get(ctx, path)
				if err != nil {
					b.Fatalf("[%s] Get failed: %v", name, err)
				}
				_, err = io.Copy(io.Discard, r)
				r.Close()
				if err != nil {
					b.Fatalf("[%s] Read failed: %v", name, err)
				}
			}

			b.StopTimer()

			// Cleanup
			err := store.DeleteAll(ctx, "bench")
			require.NoError(b, err, "[%s] DeleteAll cleanup failed", name)
		})
	}
}
