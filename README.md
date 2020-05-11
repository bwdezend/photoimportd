# photoimportd

A daemon to run on macOS and backup Photos Library Masters to a remote storage location, sorted by EXIF dates.

Usage of photoimportd:
  -db string
    	Database path (default "~/.photoimportd.db")
  -debug
    	Turn on debug level logging
  -dryrun
    	Dry-run
  -dst string
    	Long term storage path (default "/mnt/nfs/photos/MasterImages")
  -sleep int
    	Sleep interval between src scans (default 90)
  -src string
    	Photo library Master path (default "~/Pictures/Photos Library.photoslibrary/Masters")
  -trace
    	Turn on trace level logging
  -workers int
    	Number of worker threads to run concurrently (default 5)
