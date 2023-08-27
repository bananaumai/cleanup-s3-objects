package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
)

const optMaxKeys = "max-keys"
const optQuiet = "quiet"
const optTimeout = "timeout"

const defaultMaxKeys = 1000
const defaultQuiet = false
const defaultTimeout = 0

func printUsage() {
	cmd := os.Args[0]
	_, _ = fmt.Fprintf(os.Stderr, "Usage: %s [-%s <n: 1-1000>] [-%s] [-%s <duration>] <bucket>\n", cmd, optMaxKeys, optQuiet, optTimeout)
	flag.PrintDefaults()
}

func main() {
	var (
		maxKeys int64
		quiet   bool
		timeout time.Duration
	)

	flag.Int64Var(&maxKeys, optMaxKeys, defaultMaxKeys, "max-keys parameter for the S3 ListObjectVersions API")
	flag.BoolVar(&quiet, optQuiet, defaultQuiet, "suppress logging messages")
	flag.DurationVar(&timeout, optTimeout, defaultTimeout, "set timeout for the operation")
	flag.Parse()

	if quiet {
		log.SetOutput(io.Discard)
	} else {
		log.SetOutput(os.Stderr)
	}

	bucket := flag.Arg(0)
	if bucket == "" {
		printUsage()
		os.Exit(1)
	}

	c := cleaner{
		s3Client: &s3cli{
			s3API: s3.New(session.Must(session.NewSession())),
		},
		bucket:  bucket,
		maxKeys: 1000,
	}

	ctx := context.Background()
	if timeout > 0 {
		ctxWithTimeout, cancel := context.WithTimeout(ctx, 5*time.Minute)
		defer cancel()
		ctx = ctxWithTimeout
	}

	deletedVersions, deletedDeleteMarker, err := c.cleanup(ctx)
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}

	_, _ = fmt.Fprintf(os.Stdout, "Purged %d versions of objects and %d object delete makers from s3://%s\n", deletedVersions, deletedDeleteMarker, bucket)
}

type (
	cleaner struct {
		s3Client

		bucket  string
		maxKeys int64
	}

	s3Client interface {
		listObjectVersions(ctx context.Context, bucket string, maxKeys int64, keyMarker, versionIdMarker *string) (versions []*object, deleteMarkers []*object, nextKeyMarker, nextVersionIdMarker *string, err error)
		deleteObjects(ctx context.Context, bucket string, objects []*object) error
	}

	s3cli struct {
		s3API s3iface.S3API
	}

	object struct {
		Key       string
		VersionId string
	}
)

func (c *cleaner) cleanup(ctx context.Context) (deletedVersion, deletedDeleteMarker int, err error) {
	var (
		versions            []*object
		deleteMarkers       []*object
		nextKeyMarker       *string
		nextVersionIdMarker *string
	)

	for {
		versions, deleteMarkers, nextKeyMarker, nextVersionIdMarker, err = c.listObjectVersions(ctx, c.bucket, c.maxKeys, nextKeyMarker, nextVersionIdMarker)
		if err != nil {
			return deletedVersion, deletedDeleteMarker, fmt.Errorf("failed to list object versions: %w", err)
		}

		if len(versions) > 0 {
			if err := c.deleteVersions(ctx, versions); err != nil {
				return deletedVersion, deletedDeleteMarker, fmt.Errorf("failed to delete versions: %w", err)
			}
			deletedVersion += len(versions)
		}

		if len(deleteMarkers) > 0 {
			if err := c.deleteDeleteMarkers(ctx, deleteMarkers); err != nil {
				return 0, 0, fmt.Errorf("failed to delete delete markers: %w", err)
			}
			deletedDeleteMarker += len(deleteMarkers)
		}

		// probably it's not necessary to check the length of versions and deleteMarkers;
		// i.e., if nextKeyMarker and nextVersionIdMarker are nil, it means that there are no more versions and delete markers.
		// but just in case, check the length of versions and deleteMarkers, which might cause one more (unnecessary) API call.
		if len(versions) == 0 && len(deleteMarkers) == 0 && nextKeyMarker == nil && nextVersionIdMarker == nil {
			break
		}
	}

	return deletedVersion, deletedDeleteMarker, nil
}

func (c *cleaner) deleteVersions(ctx context.Context, versions []*object) error {
	if err := c.deleteObjects(ctx, c.bucket, versions); err != nil {
		return fmt.Errorf("failed to delete versions: %w", err)
	}
	log.Printf("Deleted %d versions", len(versions))
	return nil
}

func (c *cleaner) deleteDeleteMarkers(ctx context.Context, deleteMarkers []*object) error {
	if err := c.deleteObjects(ctx, c.bucket, deleteMarkers); err != nil {
		return fmt.Errorf("failed to delete delete markers: %w", err)
	}
	log.Printf("Deleted %d delete markers", len(deleteMarkers))
	return nil
}

func (c *s3cli) listObjectVersions(ctx context.Context, bucket string, maxKeys int64, keyMarker, versionIdMarker *string) (versions []*object, deleteMarkers []*object, nextKeyMarker, nextVersionIdMarker *string, err error) {
	input := s3.ListObjectVersionsInput{
		Bucket:          aws.String(bucket),
		MaxKeys:         aws.Int64(maxKeys),
		KeyMarker:       keyMarker,
		VersionIdMarker: versionIdMarker,
	}

	logMsg := "Calling ListObjectVersions API"
	if keyMarker != nil {
		logMsg += fmt.Sprintf(":keyMarker=%s", *keyMarker)
	}
	if versionIdMarker != nil {
		logMsg += fmt.Sprintf(":versionIdMarker=%s", *versionIdMarker)
	}
	log.Printf(logMsg)

	out, err := c.s3API.ListObjectVersionsWithContext(ctx, &input)
	if err != nil {
		return nil, nil, nil, nil, fmt.Errorf("ListObjectVersions API error: %w", err)
	}
	log.Printf("Retrieved %d versions and %d deleteMarkers from s3://%s", len(out.Versions), len(out.DeleteMarkers), bucket)

	if len(out.Versions) > 0 {
		versions = make([]*object, len(out.Versions))
		for i, v := range out.Versions {
			versions[i] = &object{
				Key:       *v.Key,
				VersionId: *v.VersionId,
			}
		}
	}

	if len(out.DeleteMarkers) > 0 {
		deleteMarkers = make([]*object, len(out.DeleteMarkers))
		for i, d := range out.DeleteMarkers {
			deleteMarkers[i] = &object{
				Key:       *d.Key,
				VersionId: *d.VersionId,
			}
		}
	}

	return versions, deleteMarkers, out.NextKeyMarker, out.NextVersionIdMarker, nil
}

func (c *s3cli) deleteObjects(ctx context.Context, bucket string, objects []*object) error {
	ids := make([]*s3.ObjectIdentifier, len(objects))
	for i, o := range objects {
		ids[i] = &s3.ObjectIdentifier{
			Key:       aws.String(o.Key),
			VersionId: aws.String(o.VersionId),
		}
	}
	input := s3.DeleteObjectsInput{
		Bucket: aws.String(bucket),
		Delete: &s3.Delete{
			Objects: ids,
		},
	}

	log.Printf("Calling DeleteObjects API with %d objects", len(objects))
	_, err := c.s3API.DeleteObjectsWithContext(ctx, &input)
	if err != nil {
		return fmt.Errorf("DeleteObjects API error: %w", err)
	}

	return nil
}
