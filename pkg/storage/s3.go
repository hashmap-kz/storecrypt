package storage

import (
	"context"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
)

type s3Storage struct {
	client   *s3.Client
	bucket   string
	prefix   string
	uploader *manager.Uploader
}

var _ Storage = &s3Storage{}

func NewS3Storage(client *s3.Client, bucket, prefix string) Storage {
	return &s3Storage{
		client:   client,
		bucket:   bucket,
		prefix:   strings.TrimPrefix(prefix, "/"),
		uploader: CreateUploader(client, 5242880, 2), // TODO:cfg
	}
}

func (s *s3Storage) fullPath(path string) string {
	return filepath.ToSlash(filepath.Join(s.prefix, path))
}

// CreateUploader creates a new S3 uploader with the given part size and concurrency
func CreateUploader(client *s3.Client, partsize int64, concurrency int) *manager.Uploader {
	return manager.NewUploader(client, func(u *manager.Uploader) {
		u.PartSize = partsize
		u.Concurrency = concurrency
	})
}

func (s *s3Storage) Put(ctx context.Context, remotePath string, r io.Reader) error {
	remotePath = s.fullPath(remotePath)

	objInput := &s3.PutObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(remotePath),
		Body:   r,
	}

	_, err := s.uploader.Upload(ctx, objInput)
	if err != nil {
		return err
	}
	return nil
}

func (s *s3Storage) Get(ctx context.Context, remotePath string) (io.ReadCloser, error) {
	remotePath = s.fullPath(remotePath)

	out, err := s.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(remotePath),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to read object from S3: %w", err)
	}
	return out.Body, nil
}

func (s *s3Storage) List(ctx context.Context, remotePath string) ([]string, error) {
	fullPath := s.fullPath(remotePath)
	var objects []string

	paginator := s3.NewListObjectsV2Paginator(s.client, &s3.ListObjectsV2Input{
		Bucket: aws.String(s.bucket),
		Prefix: aws.String(fullPath),
	})

	// Iterate over pages of results
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to get page: %w", err)
		}

		for _, obj := range page.Contents {
			rel, err := filepath.Rel(s.prefix, *obj.Key)
			if err != nil {
				return nil, err
			}
			objects = append(objects, rel)
		}
	}

	return objects, nil
}

func (s *s3Storage) ListInfo(ctx context.Context, remotePath string) ([]FileInfo, error) {
	fullPath := s.fullPath(remotePath)
	var objects []FileInfo

	paginator := s3.NewListObjectsV2Paginator(s.client, &s3.ListObjectsV2Input{
		Bucket: aws.String(s.bucket),
		Prefix: aws.String(fullPath),
	})

	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to get page: %w", err)
		}

		// Iterate over pages of results
		for _, obj := range page.Contents {
			key := aws.ToString(obj.Key)

			// Normalize S3 keys using strings, not filepath
			rel := strings.TrimPrefix(key, s.prefix)
			rel = strings.TrimPrefix(rel, "/")

			objects = append(objects, FileInfo{
				Path:    rel,
				ModTime: aws.ToTime(obj.LastModified),
			})
		}
	}

	return objects, nil
}

func (s *s3Storage) Delete(ctx context.Context, remotePath string) error {
	fullPath := s.fullPath(remotePath)

	_, err := s.client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: &s.bucket,
		Key:    &fullPath,
	})
	return err
}

func (s *s3Storage) DeleteAll(ctx context.Context, remotePath string) error {
	return s.deleteAllVersions(ctx, remotePath)
}

func (s *s3Storage) DeleteAllBulk(ctx context.Context, paths []string) error {
	return s.deleteAllVersionsBulk(ctx, paths)
}

//nolint:unused
func (s *s3Storage) deleteAll(ctx context.Context, remotePath string) error {
	prefix := s.fullPath(remotePath)
	if prefix != "" && !endsWithSlash(prefix) {
		prefix += "/"
	}

	// List objects with that prefix
	paginator := s3.NewListObjectsV2Paginator(s.client, &s3.ListObjectsV2Input{
		Bucket: &s.bucket,
		Prefix: &prefix,
	})

	var objectsToDelete []s3types.ObjectIdentifier
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return fmt.Errorf("listing s3 objects: %w", err)
		}
		for _, obj := range page.Contents {
			objectsToDelete = append(objectsToDelete, s3types.ObjectIdentifier{Key: obj.Key})
		}
	}

	// Batch delete (max 1000 per request)
	for i := 0; i < len(objectsToDelete); i += 1000 {
		end := i + 1000
		if end > len(objectsToDelete) {
			end = len(objectsToDelete)
		}

		_, err := s.client.DeleteObjects(ctx, &s3.DeleteObjectsInput{
			Bucket: &s.bucket,
			Delete: &s3types.Delete{
				Objects: objectsToDelete[i:end],
				Quiet:   aws.Bool(true),
			},
		})
		if err != nil {
			return fmt.Errorf("deleting s3 objects: %w", err)
		}
	}

	return nil
}

func (s *s3Storage) deleteAllVersions(ctx context.Context, remotePath string) error {
	prefix := s.fullPath(remotePath)
	if prefix != "" && !endsWithSlash(prefix) {
		prefix += "/"
	}

	paginator := s3.NewListObjectVersionsPaginator(s.client, &s3.ListObjectVersionsInput{
		Bucket: &s.bucket,
		Prefix: &prefix,
	})

	var toDelete []s3types.ObjectIdentifier
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return fmt.Errorf("list object versions: %w", err)
		}

		for i := range page.Versions {
			version := page.Versions[i]
			toDelete = append(toDelete, s3types.ObjectIdentifier{
				Key:       version.Key,
				VersionId: version.VersionId,
			})
		}
		for _, marker := range page.DeleteMarkers {
			toDelete = append(toDelete, s3types.ObjectIdentifier{
				Key:       marker.Key,
				VersionId: marker.VersionId,
			})
		}
	}

	for i := 0; i < len(toDelete); i += 1000 {
		end := i + 1000
		if end > len(toDelete) {
			end = len(toDelete)
		}

		_, err := s.client.DeleteObjects(ctx, &s3.DeleteObjectsInput{
			Bucket: &s.bucket,
			Delete: &s3types.Delete{
				Objects: toDelete[i:end],
				Quiet:   aws.Bool(true),
			},
		})
		if err != nil {
			return fmt.Errorf("delete versions: %w", err)
		}
	}

	return nil
}

func (s *s3Storage) deleteAllVersionsBulk(ctx context.Context, paths []string) error {
	var objectsToDelete []s3types.ObjectIdentifier

	for _, path := range paths {
		prefix := s.fullPath(path)

		paginator := s3.NewListObjectVersionsPaginator(s.client, &s3.ListObjectVersionsInput{
			Bucket: &s.bucket,
			Prefix: &prefix,
		})

		for paginator.HasMorePages() {
			page, err := paginator.NextPage(ctx)
			if err != nil {
				return fmt.Errorf("list object versions for %q: %w", prefix, err)
			}
			for i := range page.Versions {
				version := page.Versions[i]
				objectsToDelete = append(objectsToDelete, s3types.ObjectIdentifier{
					Key:       version.Key,
					VersionId: version.VersionId,
				})
			}
			for i := range page.DeleteMarkers {
				deleteMarker := page.DeleteMarkers[i]
				objectsToDelete = append(objectsToDelete, s3types.ObjectIdentifier{
					Key:       deleteMarker.Key,
					VersionId: deleteMarker.VersionId,
				})
			}
		}
	}

	// Split into chunks of 1000 due to S3 limit per DeleteObjects request
	const batchSize = 1000
	for i := 0; i < len(objectsToDelete); i += batchSize {
		end := i + batchSize
		if end > len(objectsToDelete) {
			end = len(objectsToDelete)
		}

		_, err := s.client.DeleteObjects(ctx, &s3.DeleteObjectsInput{
			Bucket: &s.bucket,
			Delete: &s3types.Delete{
				Objects: objectsToDelete[i:end],
				Quiet:   aws.Bool(true),
			},
		})
		if err != nil {
			return fmt.Errorf("delete objects batch %d–%d: %w", i, end, err)
		}
	}

	return nil
}

func (s *s3Storage) Exists(ctx context.Context, remotePath string) (bool, error) {
	remotePath = s.fullPath(remotePath)

	_, err := s.client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(remotePath),
	})
	if err != nil {
		var nf *s3types.NotFound
		if errors.As(err, &nf) {
			return false, nil
		}
		return false, err
	}
	return true, nil // S3 has no dirs, so it's a valid file
}

func endsWithSlash(s string) bool {
	return s != "" && s[len(s)-1] == '/'
}
