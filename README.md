# mirror-intel

The intelligent mirror redirector middleware for SJTUG.

## Usage

```
./mirror-intel
```

## Detail

* mirror-intel will first query if object exists in s3 backend
* if yes, it will redirect user to s3 object storage
* if not, it will redirect user to original site, and submit task for download
* the task will download file from original site and upload it to s3 backend
