# S3 Objects Cleaner

When you want to delete a S3 bucket, you need to delete all versions of objects and delete markers.

But it's very hard to delete all versions of objects and deleteMarkers
when there are so many objects in a S3 bucket, and especially when the bucket versioning is enabled.

There are no AWS CLI commands that let you do such operation in an easy way.

`cleanup-s3-objects` allows you to delete all versions of objects and deleteMarkers in a S3 bucket.  

**This command will delete all versions of objects and deleteMarkers in the bucket.
This is the irreversible operation and can be very dangerous. Please make sure what you are going to do with your responsibility**

## Usage

```bash
$ cleanup-s3-objects [-debug] [-max-keys <n: 1-1000>] [-timeout <duration>] <bucket>
```