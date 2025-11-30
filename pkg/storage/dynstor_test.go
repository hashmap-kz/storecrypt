package storage

import (
	"bytes"
	"context"
	"errors"
	"io"
	"io/fs"
	"sort"
	"strings"
	"testing"

	"github.com/hashmap-kz/streamcrypt/pkg/codec"
	"github.com/hashmap-kz/streamcrypt/pkg/crypt/aesgcm"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// -----------------------------------------------------------------------------
// In-memory backend for unit tests
// -----------------------------------------------------------------------------

type memoryStorage struct {
	data map[string][]byte
	dirs map[string]bool
}

func newMemoryStorage() *memoryStorage {
	return &memoryStorage{
		data: make(map[string][]byte),
		dirs: make(map[string]bool),
	}
}

func (m *memoryStorage) Put(_ context.Context, path string, r io.Reader) error {
	if m.data == nil {
		m.data = make(map[string][]byte)
	}
	if m.dirs == nil {
		m.dirs = make(map[string]bool)
	}

	b, err := io.ReadAll(r)
	if err != nil {
		return err
	}
	m.data[path] = b

	// naive directory tracking
	parts := strings.Split(path, "/")
	for i := 1; i < len(parts); i++ {
		prefix := strings.Join(parts[:i], "/")
		m.dirs[prefix] = true
	}
	return nil
}

func (m *memoryStorage) Get(_ context.Context, path string) (io.ReadCloser, error) {
	b, ok := m.data[path]
	if !ok {
		return nil, fs.ErrNotExist
	}
	return io.NopCloser(bytes.NewReader(b)), nil
}

func (m *memoryStorage) List(_ context.Context, prefix string) ([]string, error) {
	var out []string
	for k := range m.data {
		if strings.HasPrefix(k, prefix) {
			out = append(out, k)
		}
	}
	sort.Strings(out)
	return out, nil
}

func (m *memoryStorage) ListInfo(_ context.Context, prefix string) ([]FileInfo, error) {
	var out []FileInfo
	for k := range m.data {
		if strings.HasPrefix(k, prefix) {
			out = append(out, FileInfo{Path: k})
		}
	}
	return out, nil
}

func (m *memoryStorage) Delete(_ context.Context, path string) error {
	if _, ok := m.data[path]; !ok {
		return fs.ErrNotExist
	}
	delete(m.data, path)
	return nil
}

func (m *memoryStorage) DeleteDir(_ context.Context, path string) error {
	for k := range m.data {
		if k == path || strings.HasPrefix(k, path+"/") {
			delete(m.data, k)
		}
	}
	return nil
}

func (m *memoryStorage) DeleteAll(_ context.Context, prefix string) error {
	for k := range m.data {
		if strings.HasPrefix(k, prefix) {
			delete(m.data, k)
		}
	}
	return nil
}

func (m *memoryStorage) DeleteAllBulk(ctx context.Context, paths []string) error {
	for _, p := range paths {
		if err := m.Delete(ctx, p); err != nil && !errors.Is(err, fs.ErrNotExist) {
			return err
		}
	}
	return nil
}

func (m *memoryStorage) Exists(_ context.Context, path string) (bool, error) {
	_, ok := m.data[path]
	return ok, nil
}

func (m *memoryStorage) ListTopLevelDirs(_ context.Context, prefix string) (map[string]bool, error) {
	// very naive: return any tracked dir starting with prefix
	out := make(map[string]bool)
	for k := range m.dirs {
		if strings.HasPrefix(k, prefix) {
			out[k] = true
		}
	}
	return out, nil
}

// -----------------------------------------------------------------------------
// NewVariadicStorage / isSupportedWriteExt
// -----------------------------------------------------------------------------

func TestNewVariadicStorage_ValidationMatrix(t *testing.T) {
	ctx := context.Background()
	_ = ctx // just to have it handy if needed later

	aes := aesgcm.NewChunkedGCMCrypter("password")
	gzipPair := &CodecPair{
		Compressor:   codec.GzipCompressor{},
		Decompressor: codec.GzipDecompressor{},
	}
	zstdPair := &CodecPair{
		Compressor:   codec.ZstdCompressor{},
		Decompressor: codec.ZstdDecompressor{},
	}

	tests := []struct {
		name     string
		alg      Algorithms
		writeExt string
		ok       bool
	}{
		{"plain-only-ok", Algorithms{}, "", true},
		{"plain-only-gz-fail", Algorithms{}, ".gz", false},
		{"gzip-ok", Algorithms{Gzip: gzipPair}, ".gz", true},
		{"gzip-gz.aes-fail-no-aes", Algorithms{Gzip: gzipPair}, ".gz.aes", false},
		{"zstd-ok", Algorithms{Zstd: zstdPair}, ".zst", true},
		{"aes-only-aes-ok", Algorithms{AES: aes}, ".aes", true},
		{"aes-only-gz-fail", Algorithms{AES: aes}, ".gz", false},
		{"gzip-aes-gz.aes-ok", Algorithms{Gzip: gzipPair, AES: aes}, ".gz.aes", true},
		{"zstd-aes-zst.aes-ok", Algorithms{Zstd: zstdPair, AES: aes}, ".zst.aes", true},
		{"gzip-zstd-aes-gz-ok", Algorithms{Gzip: gzipPair, Zstd: zstdPair, AES: aes}, ".gz", true},
		{"gzip-zstd-aes-zst.ok", Algorithms{Gzip: gzipPair, Zstd: zstdPair, AES: aes}, ".zst", true},
		{"unknown-ext-fail", Algorithms{Gzip: gzipPair, AES: aes}, ".xyz", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			backend := newMemoryStorage()
			vs, err := NewVariadicStorage(backend, tt.alg, tt.writeExt)
			if tt.ok {
				require.NoError(t, err)
				require.NotNil(t, vs)
			} else {
				require.Error(t, err)
				require.Nil(t, vs)
			}
		})
	}
}

// -----------------------------------------------------------------------------
// supportedExts
// -----------------------------------------------------------------------------

func TestSupportedExts_OrderAndContent(t *testing.T) {
	aes := aesgcm.NewChunkedGCMCrypter("password")
	gzipPair := &CodecPair{
		Compressor:   codec.GzipCompressor{},
		Decompressor: codec.GzipDecompressor{},
	}
	zstdPair := &CodecPair{
		Compressor:   codec.ZstdCompressor{},
		Decompressor: codec.ZstdDecompressor{},
	}

	tests := []struct {
		name string
		alg  Algorithms
		want []string
	}{
		{
			name: "plain-only",
			alg:  Algorithms{},
			want: []string{""},
		},
		{
			name: "gzip-only",
			alg:  Algorithms{Gzip: gzipPair},
			want: []string{".gz", ""},
		},
		{
			name: "zstd-only",
			alg:  Algorithms{Zstd: zstdPair},
			want: []string{".zst", ""},
		},
		{
			name: "aes-only",
			alg:  Algorithms{AES: aes},
			want: []string{".aes", ""},
		},
		{
			name: "gzip-aes",
			alg:  Algorithms{Gzip: gzipPair, AES: aes},
			want: []string{".gz.aes", ".gz", ".aes", ""},
		},
		{
			name: "zstd-aes",
			alg:  Algorithms{Zstd: zstdPair, AES: aes},
			want: []string{".zst.aes", ".zst", ".aes", ""},
		},
		{
			name: "gzip-zstd-aes",
			alg:  Algorithms{Gzip: gzipPair, Zstd: zstdPair, AES: aes},
			want: []string{".gz.aes", ".zst.aes", ".gz", ".zst", ".aes", ""},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			vs := &VariadicStorage{alg: tt.alg}
			got := vs.supportedExts()
			assert.Equal(t, tt.want, got)
		})
	}
}

// -----------------------------------------------------------------------------
// transformsFromName
// -----------------------------------------------------------------------------

func TestTransformsFromName_AllExtensionCombos(t *testing.T) {
	aes := aesgcm.NewChunkedGCMCrypter("password")
	gzipPair := &CodecPair{
		Compressor:   codec.GzipCompressor{},
		Decompressor: codec.GzipDecompressor{},
	}
	zstdPair := &CodecPair{
		Compressor:   codec.ZstdCompressor{},
		Decompressor: codec.ZstdDecompressor{},
	}

	alg := Algorithms{
		Gzip: gzipPair,
		Zstd: zstdPair,
		AES:  aes,
	}
	vs := &VariadicStorage{alg: alg}

	type wantFlags struct {
		compress bool
		aes      bool
	}

	tests := []struct {
		name string
		path string
		want wantFlags
	}{
		{"plain", "file", wantFlags{compress: false, aes: false}},
		{"gzip", "file.gz", wantFlags{compress: true, aes: false}},
		{"zstd", "file.zst", wantFlags{compress: true, aes: false}},
		{"aes-only", "file.aes", wantFlags{compress: false, aes: true}},
		{"gzip-aes", "file.gz.aes", wantFlags{compress: true, aes: true}},
		{"zstd-aes", "file.zst.aes", wantFlags{compress: true, aes: true}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tr := vs.transformsFromName(tt.path)

			gotCompress := tr.compressor != nil && tr.decompressor != nil
			gotAES := tr.crypter != nil

			assert.Equal(t, tt.want.compress, gotCompress, "compress flag mismatch")
			assert.Equal(t, tt.want.aes, gotAES, "aes flag mismatch")
		})
	}
}

// -----------------------------------------------------------------------------
// encodePath / decodePath
// -----------------------------------------------------------------------------

func TestEncodeDecodePath_RoundTrip(t *testing.T) {
	aes := aesgcm.NewChunkedGCMCrypter("password")
	gzipPair := &CodecPair{
		Compressor:   codec.GzipCompressor{},
		Decompressor: codec.GzipDecompressor{},
	}
	zstdPair := &CodecPair{
		Compressor:   codec.ZstdCompressor{},
		Decompressor: codec.ZstdDecompressor{},
	}

	alg := Algorithms{
		Gzip: gzipPair,
		Zstd: zstdPair,
		AES:  aes,
	}

	exts := []string{"", ".gz", ".zst", ".aes", ".gz.aes", ".zst.aes"}
	base := "some/dir/file"

	for _, ext := range exts {
		t.Run("writeExt="+ext, func(t *testing.T) {
			vs, err := NewVariadicStorage(newMemoryStorage(), alg, ext)
			if ext == ".gz" || ext == ".zst" || ext == ".aes" || ext == ".gz.aes" || ext == ".zst.aes" {
				require.NoError(t, err)
			}

			// encodePath should append writeExt
			stored := vs.encodePath(base)
			assert.Equal(t, base+ext, stored)

			// decodePath should remove any *known* extension combination
			decoded := vs.decodePath(base + ext)
			assert.Equal(t, base, decoded)
		})
	}
}

// -----------------------------------------------------------------------------
// findExistingName
// -----------------------------------------------------------------------------

func TestFindExistingName_Priority(t *testing.T) {
	aes := aesgcm.NewChunkedGCMCrypter("password")
	gzipPair := &CodecPair{
		Compressor:   codec.GzipCompressor{},
		Decompressor: codec.GzipDecompressor{},
	}
	zstdPair := &CodecPair{
		Compressor:   codec.ZstdCompressor{},
		Decompressor: codec.ZstdDecompressor{},
	}

	alg := Algorithms{
		Gzip: gzipPair,
		Zstd: zstdPair,
		AES:  aes,
	}

	ctx := context.Background()

	t.Run("none-exist", func(t *testing.T) {
		mem := newMemoryStorage()
		vs, err := NewVariadicStorage(mem, alg, ".gz")
		require.NoError(t, err)

		_, err = vs.findExistingName(ctx, "file")
		require.ErrorIs(t, err, fs.ErrNotExist)
	})

	t.Run("plain-vs-gz-priority", func(t *testing.T) {
		mem := newMemoryStorage()
		mem.data["file"] = []byte("plain")
		mem.data["file.gz"] = []byte("gz")

		vs, err := NewVariadicStorage(mem, alg, ".gz")
		require.NoError(t, err)

		stored, err := vs.findExistingName(ctx, "file")
		require.NoError(t, err)
		assert.Equal(t, "file.gz", stored) // .gz wins over plain
	})

	t.Run("gz.aes-highest-priority", func(t *testing.T) {
		mem := newMemoryStorage()
		mem.data["file"] = []byte("plain")
		mem.data["file.gz"] = []byte("gz")
		mem.data["file.gz.aes"] = []byte("gz.aes")

		vs, err := NewVariadicStorage(mem, alg, ".gz.aes")
		require.NoError(t, err)

		stored, err := vs.findExistingName(ctx, "file")
		require.NoError(t, err)
		assert.Equal(t, "file.gz.aes", stored)
	})
}

// -----------------------------------------------------------------------------
// Put/Get roundtrip for all variants (using real gzip/zstd/aes)
// -----------------------------------------------------------------------------

func TestVariadicStorage_PutGet_RoundTrip_AllWriteExts(t *testing.T) {
	ctx := context.Background()

	aes := aesgcm.NewChunkedGCMCrypter("password")
	gzipPair := &CodecPair{
		Compressor:   codec.GzipCompressor{},
		Decompressor: codec.GzipDecompressor{},
	}
	zstdPair := &CodecPair{
		Compressor:   codec.ZstdCompressor{},
		Decompressor: codec.ZstdDecompressor{},
	}

	tests := []struct {
		name     string
		alg      Algorithms
		writeExt string
	}{
		{"plain", Algorithms{}, ""},
		{"gzip", Algorithms{Gzip: gzipPair}, ".gz"},
		{"zstd", Algorithms{Zstd: zstdPair}, ".zst"},
		{"aes-only", Algorithms{AES: aes}, ".aes"},
		{"gzip-aes", Algorithms{Gzip: gzipPair, AES: aes}, ".gz.aes"},
		{"zstd-aes", Algorithms{Zstd: zstdPair, AES: aes}, ".zst.aes"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mem := newMemoryStorage()
			vs, err := NewVariadicStorage(mem, tt.alg, tt.writeExt)
			require.NoError(t, err)

			path := "wal/000000010000000000000001"
			content := []byte("hello variadic storage")

			// Put by logical name
			require.NoError(t, vs.Put(ctx, path, bytes.NewReader(content)))

			// Ensure a single physical object with encoded name exists
			expectedKey := path + tt.writeExt
			require.Contains(t, mem.data, expectedKey)
			require.Len(t, mem.data, 1)

			// Exists by logical name
			ok, err := vs.Exists(ctx, path)
			require.NoError(t, err)
			assert.True(t, ok)

			// Get by logical name
			rc, err := vs.Get(ctx, path)
			require.NoError(t, err)
			got, err := io.ReadAll(rc)
			rc.Close()
			require.NoError(t, err)
			assert.Equal(t, content, got)

			// Get by fully encoded name should also work
			rc2, err := vs.Get(ctx, expectedKey)
			require.NoError(t, err)
			got2, err := io.ReadAll(rc2)
			rc2.Close()
			require.NoError(t, err)
			assert.Equal(t, content, got2)
		})
	}
}

// -----------------------------------------------------------------------------
// Delete / DeleteAllBulk / Exists
// -----------------------------------------------------------------------------

func TestVariadicStorage_Delete_RemovesAllVariants(t *testing.T) {
	ctx := context.Background()

	aes := aesgcm.NewChunkedGCMCrypter("password")
	gzipPair := &CodecPair{
		Compressor:   codec.GzipCompressor{},
		Decompressor: codec.GzipDecompressor{},
	}
	alg := Algorithms{
		Gzip: gzipPair,
		AES:  aes,
	}

	mem := newMemoryStorage()
	mem.data["wal/seg"] = []byte("plain")
	mem.data["wal/seg.gz"] = []byte("gz")
	mem.data["wal/seg.gz.aes"] = []byte("gz.aes")
	mem.data["wal/seg.aes"] = []byte("aes")

	vs, err := NewVariadicStorage(mem, alg, ".gz.aes")
	require.NoError(t, err)

	require.NoError(t, vs.Delete(ctx, "wal/seg"))

	for k := range mem.data {
		if strings.HasPrefix(k, "wal/seg") {
			t.Fatalf("expected no wal/seg* variants, found %q", k)
		}
	}
}

func TestVariadicStorage_DeleteAllBulk_LogicalNames(t *testing.T) {
	ctx := context.Background()

	gzipPair := &CodecPair{
		Compressor:   codec.GzipCompressor{},
		Decompressor: codec.GzipDecompressor{},
	}
	alg := Algorithms{
		Gzip: gzipPair,
	}

	mem := newMemoryStorage()
	mem.data["bulk/f1.gz"] = []byte("1")
	mem.data["bulk/f2.gz"] = []byte("2")
	mem.data["bulk/f3.gz"] = []byte("3")

	vs, err := NewVariadicStorage(mem, alg, ".gz")
	require.NoError(t, err)

	err = vs.DeleteAllBulk(ctx, []string{"bulk/f1", "bulk/f3"})
	require.NoError(t, err)

	// Only bulk/f2.gz should remain
	_, ok1 := mem.data["bulk/f1.gz"]
	_, ok3 := mem.data["bulk/f3.gz"]
	_, ok2 := mem.data["bulk/f2.gz"]

	assert.False(t, ok1)
	assert.False(t, ok3)
	assert.True(t, ok2)
}

func TestVariadicStorage_Exists_AnyVariant(t *testing.T) {
	ctx := context.Background()

	aes := aesgcm.NewChunkedGCMCrypter("password")
	gzipPair := &CodecPair{
		Compressor:   codec.GzipCompressor{},
		Decompressor: codec.GzipDecompressor{},
	}
	alg := Algorithms{
		Gzip: gzipPair,
		AES:  aes,
	}

	mem := newMemoryStorage()
	mem.data["wal/seg.gz.aes"] = []byte("data")

	vs, err := NewVariadicStorage(mem, alg, ".gz.aes")
	require.NoError(t, err)

	ok, err := vs.Exists(ctx, "wal/seg")
	require.NoError(t, err)
	assert.True(t, ok)

	ok2, err := vs.Exists(ctx, "wal/other")
	require.NoError(t, err)
	assert.False(t, ok2)
}

// -----------------------------------------------------------------------------
// List / ListInfo / ListTopLevelDirs
// -----------------------------------------------------------------------------

func TestVariadicStorage_List_RewritesLogicalNames_NoDedup(t *testing.T) {
	ctx := context.Background()

	aes := aesgcm.NewChunkedGCMCrypter("password")
	gzipPair := &CodecPair{
		Compressor:   codec.GzipCompressor{},
		Decompressor: codec.GzipDecompressor{},
	}
	alg := Algorithms{
		Gzip: gzipPair,
		AES:  aes,
	}

	mem := newMemoryStorage()
	mem.data["p/a.gz"] = []byte("1")
	mem.data["p/a.gz.aes"] = []byte("2")
	mem.data["p/b"] = []byte("3")
	mem.data["p/b.aes"] = []byte("4")

	vs, err := NewVariadicStorage(mem, alg, ".gz.aes")
	require.NoError(t, err)

	list, err := vs.List(ctx, "p")
	require.NoError(t, err)

	// decodePath will map:
	//   p/a.gz      -> p/a
	//   p/a.gz.aes  -> p/a
	//   p/b         -> p/b
	//   p/b.aes     -> p/b
	// Important: no dedup => 4 results.
	assert.ElementsMatch(t, []string{"p/a", "p/a", "p/b", "p/b"}, list)
}

func TestVariadicStorage_ListInfo_RewritesPath(t *testing.T) {
	ctx := context.Background()

	aes := aesgcm.NewChunkedGCMCrypter("password")
	gzipPair := &CodecPair{
		Compressor:   codec.GzipCompressor{},
		Decompressor: codec.GzipDecompressor{},
	}
	alg := Algorithms{
		Gzip: gzipPair,
		AES:  aes,
	}

	mem := newMemoryStorage()
	mem.data["p/a.gz.aes"] = []byte("1")
	mem.data["p/c"] = []byte("2")

	vs, err := NewVariadicStorage(mem, alg, ".gz.aes")
	require.NoError(t, err)

	info, err := vs.ListInfo(ctx, "p")
	require.NoError(t, err)

	//nolint:prealloc
	var paths []string
	for _, fi := range info {
		paths = append(paths, fi.Path)
	}

	assert.ElementsMatch(t, []string{"p/a", "p/c"}, paths)
}

func TestVariadicStorage_ListTopLevelDirs_Delegates(t *testing.T) {
	ctx := context.Background()

	alg := Algorithms{} // plain is fine
	mem := newMemoryStorage()
	mem.dirs["root/app"] = true
	mem.dirs["root/db"] = true
	mem.dirs["other"] = true

	vs, err := NewVariadicStorage(mem, alg, "")
	require.NoError(t, err)

	got, err := vs.ListTopLevelDirs(ctx, "root")
	require.NoError(t, err)

	assert.Equal(t, map[string]bool{
		"root/app": true,
		"root/db":  true,
	}, got)
}
