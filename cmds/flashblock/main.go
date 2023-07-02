package main

import (
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/akmistry/go-nbd"

	"github.com/akmistry/flashblock"
	"github.com/akmistry/flashblock/simpleftl"
)

const (
	blockSize = 4096

	megabyte = 1024 * 1024
	gigabyte = 1024 * megabyte

	defaultEraseBlockSize = 1 * megabyte
	defaultDeviceSize     = 16 * gigabyte
)

var (
	deviceFlag = flag.String(
		"device", "/dev/nbd0", "Path to /deb/nbdX device.")
	deviceSizeFlag = flag.Int64(
		"device-size", defaultDeviceSize, "Block device size (bytes)")
	eraseBlockSizeFlag = flag.Int64(
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

	chip := flashblock.NewChip(*eraseBlockSizeFlag, *deviceSizeFlag, f)
	ftl := simpleftl.New(blockSize, chip)

	opts := nbd.BlockDeviceOptions{
		BlockSize: blockSize,
	}
	nbdDevice, err := nbd.NewServer(*deviceFlag, ftl, ftl.Size(), opts)
	if err != nil {
		log.Panicln(err)
	}

	ch := make(chan os.Signal, 1)
	signal.Notify(ch, os.Interrupt, syscall.SIGQUIT, syscall.SIGTERM)
	go func() {
		<-ch
		nbdDevice.Disconnect()
	}()

	log.Println("nbd: ", nbdDevice.Run())
}
