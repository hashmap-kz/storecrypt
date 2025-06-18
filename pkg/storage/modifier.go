package storage

import (
	"context"
	"io"
	"path/filepath"
	"strings"

	"github.com/hashmap-kz/streamcrypt/pkg/codec"
	"github.com/hashmap-kz/streamcrypt/pkg/crypt"
	"github.com/hashmap-kz/streamcrypt/pkg/pipe"
)

type TransformingStorage struct {
	Backend      Storage
	Crypter      crypt.Crypter
	Compressor   codec.Compressor
	Decompressor codec.Decompressor
}

var _ Storage = &TransformingStorage{}

func (ts *TransformingStorage) Put(ctx context.Context, path string, r io.Reader) error {
	transformed, err := ts.wrapWrite(r)
	if err != nil {
		return err
	}
	return ts.Backend.Put(ctx, ts.encodePath(path), transformed)
}

func (ts *TransformingStorage) Get(ctx context.Context, path string) (io.ReadCloser, error) {
	// Fetch from backend
	encodePath := ts.encodePath(path)
	rc, err := ts.Backend.Get(ctx, encodePath)
	if err != nil {
		return nil, err
	}
	// Wrap with decrypt + decompress
	return ts.wrapRead(rc)
}

func (ts *TransformingStorage) List(ctx context.Context, prefix string) ([]string, error) {
	files, err := ts.Backend.List(ctx, prefix)
	if err != nil {
		return nil, err
	}
	for i := range files {
		files[i] = ts.decodePath(files[i])
	}
	return files, nil
}

func (ts *TransformingStorage) ListInfo(ctx context.Context, prefix string) ([]FileInfo, error) {
	files, err := ts.Backend.ListInfo(ctx, prefix)
	if err != nil {
		return nil, err
	}
	for i := range files {
		files[i].Path = ts.decodePath(files[i].Path)
	}
	return files, nil
}

func (ts *TransformingStorage) Delete(ctx context.Context, path string) error {
	return ts.Backend.Delete(ctx, ts.encodePath(path))
}

func (ts *TransformingStorage) DeleteDir(ctx context.Context, path string) error {
	return ts.Backend.DeleteDir(ctx, path)
}

func (ts *TransformingStorage) DeleteAll(ctx context.Context, path string) error {
	return ts.Backend.DeleteAll(ctx, path)
}

func (ts *TransformingStorage) DeleteAllBulk(ctx context.Context, paths []string) error {
	for i := range paths {
		paths[i] = ts.encodePath(paths[i])
	}
	return ts.Backend.DeleteAllBulk(ctx, paths)
}

func (ts *TransformingStorage) Exists(ctx context.Context, path string) (bool, error) {
	return ts.Backend.Exists(ctx, ts.encodePath(path))
}

func (ts *TransformingStorage) ListTopLevelDirs(ctx context.Context, prefix string) (map[string]bool, error) {
	return ts.Backend.ListTopLevelDirs(ctx, prefix)
}

// compress/encrypt wrappers

func (ts *TransformingStorage) wrapWrite(in io.Reader) (io.Reader, error) {
	return pipe.CompressAndEncryptOptional(in, ts.Compressor, ts.Crypter)
}

func (ts *TransformingStorage) wrapRead(in io.Reader) (io.ReadCloser, error) {
	return pipe.DecryptAndDecompressOptional(in, ts.Crypter, ts.Decompressor)
}

// utils

func (ts *TransformingStorage) encodePath(path string) string {
	return filepath.ToSlash(path + ts.getFileExt())
}

func (ts *TransformingStorage) decodePath(path string) string {
	return strings.TrimSuffix(path, ts.getFileExt())
}

func (ts *TransformingStorage) getFileExt() string {
	ext := ""
	if ts.Compressor != nil {
		ext += ts.Compressor.FileExtension()
	}
	if ts.Crypter != nil {
		ext += ts.Crypter.FileExtension()
	}
	return ext
}
