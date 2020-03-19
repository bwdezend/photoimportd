package main

import (
	"crypto/sha256"
	"fmt"
	bolt "go.etcd.io/bbolt"
	"io"
	"log"
	"os"
	"path/filepath"
	"time"
)

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
			fmt.Printf("%d Hashing unseen file: %s\n", len(jobs), j)
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

			f, err := os.Open(j)
			if err != nil {
				log.Fatal(err)
			}
			fmt.Printf("%d Hashing unseen file: %s\n", len(jobs), j)
			if _, err := io.Copy(h, f); err != nil {
				log.Fatal(err)
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

func walkFilePath(path string, jobs chan<- string) {
	err := filepath.Walk(path, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			fmt.Printf("prevent panic by handling failure accessing a path %q: %v\n", path, err)
			// return err
		}

		if info.IsDir() == false {
			jobs <- path
		}

		return nil
	})

	if err != nil {
		fmt.Println("kaboom: ", err)
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

	walkFilePath("/mnt/nfs/photos/MasterImages", nfs)

	for true {
		walkFilePath("/Users/bwdezend/Pictures/Photos Library.photoslibrary/Masters/2020", jobs)
		fmt.Println("looping")
		time.Sleep(time.Second * 90)
	}
}
