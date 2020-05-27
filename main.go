package main

import (
	"crypto/sha256"
	"flag"
	"fmt"
	"io"
	"os"
	"os/user"
	"path/filepath"
	"time"

	"github.com/rwcarlsen/goexif/exif"
	log "github.com/sirupsen/logrus"
	bolt "go.etcd.io/bbolt"
)

var usr, usrErr = user.Current()

// Assume this is a macOS host that's using iCloud Photo Library
//  _and_ that all photos and videos will be routed through here
var srcPath = flag.String("src", usr.HomeDir+"/Pictures/Photos Library.photoslibrary/Masters", "Photo library Master path")

var dstPath = flag.String("dst", "/mnt/nfs/photos/MasterImages", "Long term storage path")
var dbPath = flag.String("db", usr.HomeDir+"/.photoimportd.db", "Database path")

var debugEnabled = flag.Bool("debug", false, "Turn on debug level logging")
var traceEnabled = flag.Bool("trace", false, "Turn on trace level logging")
var promEnabled = flag.Bool("metrics", false, "Enable prometheus metrics")
var promPort = flag.Int("port", 2112, "Port to bind prometheus metrics scrape to")
var dryrunEnabled = flag.Bool("dryrun", false, "Dry-run")
var sleepInterval = flag.Int("sleep", 90, "Sleep interval between src scans")
var workerCount = flag.Int("workers", 5, "Number of worker threads to run concurrently")

type fileHash struct {
	path string
	hash []byte
}

func init() {
	flag.Parse()

	// Log as JSON instead of the default ASCII formatter.
	log.SetFormatter(&log.JSONFormatter{})
	log.Warn("Starting Up")

	//usr, err := user.Current()
	if usrErr != nil {
		log.Fatal(usrErr)
	}

	// Output to stdout instead of the default stderr
	// Can be any io.Writer, see below for File example
	log.SetOutput(os.Stdout)

	if *traceEnabled {
		log.SetLevel(log.TraceLevel)
		log.Trace("Trace Logging Enabled")
	} else if *debugEnabled {
		log.SetLevel(log.DebugLevel)
		log.Debug("Debug Logging Enabled")
	} else {
		// Only log the warning severity or above.
		log.SetLevel(log.InfoLevel)
	}

	if *promEnabled {
		log.Info("Prometheus metrics enabled")
	}

	log.Info("Sleep interval set to ", *sleepInterval, " seconds")
	log.Info("Worker count set to ", *workerCount, " threads")
	log.Info("Database Path Set to: ", *dbPath)
	log.Info("Source Path Set to: ", *srcPath)
	log.Info("Destination Path to: ", *dstPath)
}

func lookupHash(path string, bucket string, db *bolt.DB) []byte {
	var hash []byte
	var exists bool
	if *promEnabled {
		hashLookups.Inc()
	}

	db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(bucket))
		v := b.Get([]byte(path))
		if v != nil {
			hash = v
			exists = true
		} else {
			exists = false
		}
		return nil
	})
	log.WithFields(log.Fields{"path": path, "exists": exists, "hash": fmt.Sprintf("%x", hash)}).Trace(fmt.Sprintf("lookupHash checked on %s", path))
	return hash
}

func hashExists(hash []byte, bucket string, db *bolt.DB) bool {
	var exists bool
	db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(bucket))
		v := b.Get([]byte(hash))
		if v != nil {
			exists = true
		} else {
			exists = false
		}
		return nil
	})
	log.WithFields(log.Fields{"hashExists": exists, "bucket": bucket, "checkedHash": fmt.Sprintf("%x", hash)}).Trace(fmt.Sprintf("hashExists: %v", exists))
	return exists
}

func updateDstPathDB(fh fileHash, db *bolt.DB) {
	db.Update(func(tx *bolt.Tx) error {
		h2p := tx.Bucket([]byte("dstHash2Path"))
		err := h2p.Put(fh.hash, []byte(fh.path))
		if err != nil {
			fmt.Println("Error!")
		}
		p2h := tx.Bucket([]byte("dstPath2Hash"))
		err = p2h.Put([]byte(fh.path), fh.hash)
		if err != nil {
			fmt.Println("Error!")
		}
		log.WithFields(log.Fields{"path": fh.path, "hash": fmt.Sprintf("%x", fh.hash)}).Debug("Adding unseen dstPath file to database")
		return nil
	})
}

func updateSrcPathDB(fh fileHash, db *bolt.DB) {
	db.Update(func(tx *bolt.Tx) error {
		seen := tx.Bucket([]byte("srcPathSeen"))
		err := seen.Put([]byte(fh.path), []byte(fh.hash))
		if err != nil {
			fmt.Println("Error!")
		}
		return nil
	})
	log.WithFields(log.Fields{"path": fh.path, "hash": fmt.Sprintf("%x", fh.hash)}).Debug("Adding unseen srcPath file to database")
}

func dstStorageWorker(id int, jobs <-chan string, results chan<- fileHash, db *bolt.DB) {
	for j := range jobs {

		lookedUpHash := lookupHash(j, "dstPath2Hash", db)

		if lookedUpHash == nil {
			log.WithFields(log.Fields{"hash": lookedUpHash, "path": j}).Trace("Looked up hash of dstPath file")

			var fh fileHash
			h := sha256.New()

			f, err := os.Open(j)
			if err != nil {
				log.Fatal(err)
			}
			log.WithFields(log.Fields{"path": j}).Debug("Hashing unseen dstPath file")
			if _, err := io.Copy(h, f); err != nil {
				log.Fatal(err)
			}
			fh.path = j
			fh.hash = h.Sum(nil)

			updateDstPathDB(fh, db)
			f.Close()
		}
	}
}

func hashFileWorker(id int, jobs <-chan string, results chan<- fileHash, db *bolt.DB) {
	for j := range jobs {
		if *promEnabled {
			filesScanned.Inc()
		}
		srcSeen := lookupHash(j, "srcPathSeen", db)
		if len(srcSeen) != 0 {
			log.WithFields(log.Fields{"hash": fmt.Sprintf("%x", srcSeen)}).Trace("srcPath check returned existing hash")
		} else {
			var fh fileHash
			var dstFh fileHash
			h := sha256.New()

			fileYear := 0
			fileMonth := 0
			fileDay := 0

			f, err := os.Open(j)
			if err != nil {
				log.Fatal(err)
			}
			if _, err := io.Copy(h, f); err != nil {
				log.WithFields(log.Fields{"path": j}).Fatal("Error copying file!", err)
			}

			fh.path = j
			fh.hash = h.Sum(nil)
			dstFh.hash = fh.hash

			dstSeen := hashExists(fh.hash, "dstHash2Path", db)
			if dstSeen == true {
				log.WithFields(log.Fields{"hash": fmt.Sprintf("%x", fh.hash)}).Trace("dstPath check returned existing hash")
			} else {
				exifRead, err := os.Open(j)
				if err != nil {
					log.Error("Error! ", err)
				}

				fileExif, err := exif.Decode(exifRead)
				if err != nil {
					log.Error("Error! ", err)
				} else {
					// Now that we have basic EXIF data from the file, we need to get the year,
					//  numeric month and day so the storage path can be constructed.
					t, err := fileExif.DateTime()
					if err != nil {
						log.WithFields(log.Fields{"path": fh.path, "hash": fmt.Sprintf("%x", fh.hash)}).Warn("Error! ", err)
					}
					fileYear = t.Year()
					fileMonth = int(t.Month())
					fileDay = t.Day()

					folderPath := fmt.Sprintf("%s/%d/%d-%02d/%d-%02d-%02d/", *dstPath, fileYear, fileYear, fileMonth, fileYear, fileMonth, fileDay)
					dstFh.path = fmt.Sprintf("%s/%d/%d-%02d/%d-%02d-%02d/%s", *dstPath, fileYear, fileYear, fileMonth, fileYear, fileMonth, fileDay, filepath.Base(j))

					if fileYear == 1 {
						// Could not detect the EXIF date data, use a hash to override
						dstFh.path = fmt.Sprintf("%s/%d/%d-%02d/%d-%02d-%02d/%x-%s", *dstPath, 0, 0, 0, 0, 0, 0, fh.hash, filepath.Base(j))
					}

					if *dryrunEnabled == false {
						os.MkdirAll(folderPath, os.ModePerm)
						log.WithFields(log.Fields{"path": fh.path, "dstPath": dstFh.path, "hash": fmt.Sprintf("%x", fh.hash)}).Info("Copying file to long term storage")
						copyFileContents(fh.path, dstFh.path)
						if *promEnabled {
							filesCopied.Inc()
						}
						updateDstPathDB(dstFh, db)
					} else {
						log.WithFields(log.Fields{"path": fh.path, "dstPath": dstFh.path, "hash": fmt.Sprintf("%x", fh.hash)}).Info("Would have copied file to long term storage")
					}
				}
			}
			if *dryrunEnabled == false {
				log.WithFields(log.Fields{"path": fh.path, "hash": fmt.Sprintf("%x", fh.hash)}).Debug("Adding file to srcPathSeen database")
				updateSrcPathDB(fh, db)
			}
			f.Close()
		}
	}
}

// CopyFile copies a file from src to dst. If src and dst files exist, and are
// the same, then return success. Otherise, attempt to create a hard link
// between the two files. If that fail, copy the file contents from src to dst.
func CopyFile(src, dst string) (err error) {
	sfi, err := os.Stat(src)
	if err != nil {
		return
	}
	if !sfi.Mode().IsRegular() {
		// cannot copy non-regular files (e.g., directories,
		// symlinks, devices, etc.)
		return fmt.Errorf("CopyFile: non-regular source file %s (%q)", sfi.Name(), sfi.Mode().String())
	}
	dfi, err := os.Stat(dst)
	if err != nil {
		if !os.IsNotExist(err) {
			return
		}
	} else {
		if !(dfi.Mode().IsRegular()) {
			return fmt.Errorf("CopyFile: non-regular destination file %s (%q)", dfi.Name(), dfi.Mode().String())
		}
		if os.SameFile(sfi, dfi) {
			return
		}
	}
	if err = os.Link(src, dst); err == nil {
		return
	}
	err = copyFileContents(src, dst)
	return
}

// copyFileContents copies the contents of the file named src to the file named
// by dst. The file will be created if it does not already exist. If the
// destination file exists, all it's contents will be replaced by the contents
// of the source file.
func copyFileContents(src, dst string) (err error) {
	in, err := os.Open(src)
	if err != nil {
		return
	}
	defer in.Close()
	out, err := os.Create(dst)
	if err != nil {
		return
	}
	defer func() {
		cerr := out.Close()
		if err == nil {
			err = cerr
		}
	}()
	if _, err = io.Copy(out, in); err != nil {
		return
	}
	err = out.Sync()
	return
}

func walkFilePath(path string, jobs chan<- string) {
	err := filepath.Walk(path, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			log.WithFields(log.Fields{"path": path, "err": err}).Warn(fmt.Printf("prevent panic by handling failure accessing a path %q: %v\n", path, err))
			// return err
		}

		if info.IsDir() == false {
			log.WithFields(log.Fields{"path": path}).Trace("Found file in path scan")
			jobs <- path
		}

		return nil
	})

	if err != nil {
		fmt.Println("filepath.Walk error: ", err)
	}
}

func setupDatabase(db *bolt.DB) {
	db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucket([]byte("srcPathSeen"))
		if err != nil {
			return fmt.Errorf("create bucket: %s", err)
		}
		return nil
	})

	db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucket([]byte("dstHash2Path"))
		if err != nil {
			return fmt.Errorf("create bucket: %s", err)
		}
		return nil
	})

	db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucket([]byte("dstPath2Hash"))
		if err != nil {
			return fmt.Errorf("create bucket: %s", err)
		}
		return nil
	})

}

func main() {
	var err error

	db, err := bolt.Open(*dbPath, 0600, nil)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	setupDatabase(db)

	if *promEnabled {
		go prometheusMetrics()
	}

	jobs := make(chan string, 200)
	results := make(chan fileHash, 10)
	dst := make(chan string, 200)

	for w := 1; w <= *workerCount; w++ {
		go hashFileWorker(w, jobs, results, db)
	}

	for w := 1; w <= *workerCount; w++ {
		go dstStorageWorker(w, dst, results, db)
	}

	// Can't enable srcPath/dstPath startup walk without more thought.
	//  Real problem is we either need to do both before entering main
	//  loop, or we need to be smart about doing lookups of where the
	//  file _would_ go after it's been written before it's written.
	//walkFilePath(*srcPath, jobs)

	for true {
		t := time.Now()

		dstPathStr := fmt.Sprintf("%s/%04d/%04d-%02d", *dstPath, t.Year(), t.Year(), int(t.Month()))
		log.Trace("Setting dstPath to ", dstPathStr)
		// Make sure dstPathStr exists before trying to walk it, happens when date rolls over and new path doesn't yet exist
		os.MkdirAll(dstPathStr, os.ModePerm)
		walkFilePath(dstPathStr, dst)

		walkPath := fmt.Sprintf("%s/%04d/%02d", *srcPath, t.Year(), int(t.Month()))
		log.Trace("Setting walkPath to ", walkPath)
		os.MkdirAll(walkPath, os.ModePerm)
		walkFilePath(walkPath, jobs)
		log.Trace("looping")
		time.Sleep(time.Second * time.Duration(*sleepInterval))
	}
}
