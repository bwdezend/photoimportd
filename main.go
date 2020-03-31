package main

import (
	"crypto/sha256"
	"fmt"
	"github.com/rwcarlsen/goexif/exif"
	bolt "go.etcd.io/bbolt"
	"io"
	"log"
	"os"
	"path/filepath"
	"time"
)

const storagePath = "/mnt/nfs/photos/golang-test"

type fileHash struct {
	path string
	hash []byte
}

func lookupHash(path string, bucket string, db *bolt.DB) []byte {
	var hash []byte
	db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(bucket))
		v := b.Get([]byte(path))
		if v != nil {
			// fmt.Printf("The hash of %s is: %x\n", path, v)
			hash = v
		}
		return nil
	})
	return hash
}

func nfsStorageWorker(id int, jobs <-chan string, results chan<- fileHash, db *bolt.DB) {
	for j := range jobs {

		h := lookupHash(j, "NFSPath2Hash", db)
		if h != nil {
			// fmt.Printf("db returned hash is %x\n", h)
		} else {
			var fh fileHash
			h := sha256.New()

			f, err := os.Open(j)
			if err != nil {
				log.Fatal(err)
			}
			// fmt.Printf("%d Hashing unseen file: %s\n", len(jobs), j)
			if _, err := io.Copy(h, f); err != nil {
				log.Fatal(err)
			}
			fh.path = j
			fh.hash = h.Sum(nil)

			fmt.Printf("Adding file [%x] to db - path %s\n", fh.hash, fh.path)

			db.Update(func(tx *bolt.Tx) error {
				h2p := tx.Bucket([]byte("NFSHash2Path"))
				err := h2p.Put([]byte(fh.hash), []byte(fh.path))
				if err != nil {
					fmt.Println("Error!")
				}
				p2h := tx.Bucket([]byte("NFSPath2Hash"))
				err = p2h.Put([]byte(fh.path), []byte(fh.hash))
				if err != nil {
					fmt.Println("Error!")
				}
				return nil
			})

			f.Close()
		}
	}
}

func hashFileWorker(id int, jobs <-chan string, results chan<- fileHash, db *bolt.DB) {
	for j := range jobs {

		h := lookupHash(j, "PhotosPath2Hash", db)

		if h != nil {
			// fmt.Printf("db returned hash is %x\n", h)
		} else {
			var fh fileHash
			h := sha256.New()

			var fileYear, fileMonth, fileDay int
			fileYear, fileMonth, fileDay = 0000, 00, 00

			f, err := os.Open(j)
			if err != nil {
				log.Fatal(err)
			}
			// fmt.Printf("%d Hashing unseen file: %s\n", len(jobs), j)
			if _, err := io.Copy(h, f); err != nil {
				log.Fatal(err)
			}

			exifRead, err := os.Open(j)
			if err != nil {
				fmt.Println("Error!", err)
			}

			fileExif, err := exif.Decode(exifRead)
			if err != nil {
				fmt.Println("Error!", err)
			} else {

				t, err := fileExif.DateTime()
				if err != nil {
					fmt.Println("Error!", err)
				}
				fileYear = t.Year()
				fileMonth = int(t.Month())
				fileDay = t.Day()

				folderPath := fmt.Sprintf("%s/%d/%d-%02d/%d-%02d-%02d/", storagePath, fileYear, fileYear, fileMonth, fileYear, fileMonth, fileDay)
				filePath := fmt.Sprintf("%s/%d/%d-%02d/%d-%02d-%02d/%s", storagePath, fileYear, fileYear, fileMonth, fileYear, fileMonth, fileDay, filepath.Base(j))
				// fmt.Println(folderPath, filePath)

				os.MkdirAll(folderPath, os.ModePerm)

				// fmt.Printf("New path: %s/%d/%d-%02d/%d-%02d-%02d/%s\n",
				//	storagePath, fileYear, fileYear, fileMonth, fileYear, fileMonth, fileDay, filepath.Base(j))
				copyFileContents(j, filePath)
				fmt.Println("copy ", j, filePath)
			}

			fh.path = j
			fh.hash = h.Sum(nil)

			fmt.Printf("Adding file [%x] to db - path %s\n", fh.hash, fh.path)

			db.Update(func(tx *bolt.Tx) error {
				h2p := tx.Bucket([]byte("PhotosHash2Path"))
				err := h2p.Put([]byte(fh.hash), []byte(fh.path))
				if err != nil {
					fmt.Println("Error!")
				}
				p2h := tx.Bucket([]byte("PhotosPath2Hash"))
				err = p2h.Put([]byte(fh.path), []byte(fh.hash))
				if err != nil {
					fmt.Println("Error!")
				}
				return nil
			})

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
			// fmt.Printf("prevent panic by handling failure accessing a path %q: %v\n", path, err)
			// return err
		}

		if info.IsDir() == false {
			jobs <- path
		}

		return nil
	})

	if err != nil {
		fmt.Println("filepath.Walk error: ", err)
	}
}

func main() {
	var workers int
	workers = 5
	fmt.Println("Starting up")

	var err error

	db, err := bolt.Open("photo.db", 0600, nil)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucket([]byte("PhotosHash2Path"))
		if err != nil {
			return fmt.Errorf("create bucket: %s", err)
		}
		return nil
	})

	db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucket([]byte("PhotosPath2Hash"))
		if err != nil {
			return fmt.Errorf("create bucket: %s", err)
		}
		return nil
	})

	db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucket([]byte("NFSHash2Path"))
		if err != nil {
			return fmt.Errorf("create bucket: %s", err)
		}
		return nil
	})

	db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucket([]byte("NFSPath2Hash"))
		if err != nil {
			return fmt.Errorf("create bucket: %s", err)
		}
		return nil
	})

	jobs := make(chan string, 200)
	results := make(chan fileHash, 10)
	nfs := make(chan string, 200)

	for w := 1; w <= workers; w++ {
		go hashFileWorker(w, jobs, results, db)
	}

	for w := 1; w <= workers; w++ {
		go nfsStorageWorker(w, nfs, results, db)
	}

	walkFilePath(storagePath, nfs)

	for true {
		walkFilePath("/Users/bwdezend/Pictures/Photos Library.photoslibrary/Masters/2020", jobs)
		fmt.Println("looping")
		time.Sleep(time.Second * 90)
	}
}
