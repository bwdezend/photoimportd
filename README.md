# photoimportd

A daemon to run on macOS and backup Photos Library Masters to a remote storage location, sorted by EXIF dates. It attempts to wait for the files to be written to the src path before hashing and comparing with the dst path to prevent large files from being copied more than once. It also reports promtheus style metrics for performance on port 2112 by default.

```
Usage of photoimportd:
  -all
      Scan all folders in -src instead of date-based scanning
  -db string
    	Database path (default "~/.photoimportd.db")
  -debug
    	Turn on debug level logging
  -dryrun
    	Dry-run
  -dst string
    	Long term storage path (default "/mnt/nfs/photos/MasterImages")
  -metrics
    	Enable prometheus metrics (default true)
  -port int
    	Port to bind prometheus metrics scrape to (default 2112)
  -rescan
    	Rescan src and dst on startup
  -sleep int
    	Sleep interval between src scans (default 90)
  -src string
    	Photo library Master path (default "~/Pictures/Photos Library.photoslibrary/Masters")
  -trace
    	Turn on trace level logging
  -workers int
    	Number of worker threads to run concurrently (default 5)
```