package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/hashmap-kz/storecrypt/pkg/clients"
	"github.com/hashmap-kz/storecrypt/pkg/storage"
	"github.com/pkg/sftp"

	"github.com/hashmap-kz/streamcrypt/pkg/codec"
	"github.com/hashmap-kz/streamcrypt/pkg/crypt/aesgcm"
)

func main() {
	ctx := context.Background()

	const (
		logicalPath = "demo/test.txt"
		content     = "Hello from storecrypt!"
	)

	// Initialize backends
	// Store content in each
	for name, st := range map[string]storage.Storage{
		"local": mustLocal(filepath.Join(os.TempDir(), "storecrypt-demo")),
		"s3":    mustS3("backups", "demo"),
		"sftp":  mustSFTP("demo"),
	} {
		fmt.Printf("\n===> Using backend: %s\n", name)

		err := st.Put(ctx, logicalPath, bytes.NewReader([]byte(content)))
		if err != nil {
			log.Fatalf("[%s] Put failed: %v", name, err)
		}

		//nolint:errcheck
		exists, _ := st.Exists(ctx, logicalPath)
		fmt.Printf("[%s] Exists: %v\n", name, exists)

		rc, err := st.Get(ctx, logicalPath)
		if err != nil {
			log.Fatalf("[%s] Get failed: %v", name, err)
		}
		//nolint:errcheck
		data, _ := io.ReadAll(rc)
		rc.Close()

		fmt.Printf("[%s] Retrieved content: %s\n", name, string(data))
	}
}

// Helpers

func mustLocal(baseDir string) storage.Storage {
	localBackend, err := storage.NewLocal(&storage.LocalStorageOpts{
		BaseDir:      filepath.Clean(baseDir),
		FsyncOnWrite: false,
	})
	if err != nil {
		log.Fatalf("init local: %v", err)
	}
	return wrap(localBackend)
}

func mustS3(bucket, prefix string) storage.Storage {
	client := createS3Client()
	return wrap(storage.NewS3Storage(client, bucket, prefix))
}

func mustSFTP(prefix string) storage.Storage {
	client := createSftpClient()
	return wrap(storage.NewSFTPStorage(client, prefix))
}

func wrap(backend storage.Storage) storage.Storage {
	return &storage.TransformingStorage{
		Backend:      backend,
		Crypter:      aesgcm.NewChunkedGCMCrypter("my-secure-password"),
		Compressor:   codec.GzipCompressor{},
		Decompressor: codec.GzipDecompressor{},
	}
}

func createS3Client() *s3.Client {
	client, err := clients.NewS3Client(&clients.S3Config{
		EndpointURL:     "https://localhost:9000",
		AccessKeyID:     "minioadmin",
		SecretAccessKey: "minioadmin123",
		Bucket:          "backups",
		Region:          "main",
		UsePathStyle:    true,
		DisableSSL:      true,
	})
	if err != nil {
		log.Fatal(err)
	}
	return client.Client()
}

func createSftpClient() *sftp.Client {
	pkeyPath := "./test/integration/environ/files/dotfiles/.ssh/id_ed25519"
	err := os.Chmod(pkeyPath, 0o600)
	if err != nil {
		log.Fatal(err)
	}
	client, err := clients.NewSFTPClient(&clients.SFTPConfig{
		Host:     "localhost",
		Port:     "2323",
		User:     "testuser",
		PkeyPath: pkeyPath,
	})
	if err != nil {
		log.Fatal(err)
	}
	return client.SFTPClient()
}
