package main

import (
	"flag"
	"log"
	"os"

	_ "github.com/akmistry/go-nbd"
)

const (
	blockSize = 4096

	defaultEraseBlockSize = 1024 * 1024
)

var (
	deviceFlag = flag.String("device", "/dev/nbd0", "Path to /deb/nbdX device.")

	eraseBlockSizeFlag = flag.Uint64(
		"erase-block-size", defaultEraseBlockSize, "Erase block size (bytes)")
)

func main() {
	flag.Parse()

	args := flag.Args()
	if len(args) != 1 {
		log.Fatal("Backing file MUST be specified")
	}

	f, err := os.OpenFile(args[0], os.O_RDWR, 0)
	if err != nil {
		log.Fatalf("Error opening backing file: %v", err)
	}
	defer f.Close()

}
