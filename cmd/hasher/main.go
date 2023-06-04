package main

import (
	"context"
	"os"

	"github.com/maciejgaleja/gosimple/pkg/storage"
	"github.com/maciejgaleja/gosimple/pkg/storage/filesystem"
	"github.com/maciejgaleja/gosimple/pkg/types"
	"github.com/maciejgaleja/the-hasher/pkg/hasher"
	"github.com/sirupsen/logrus"
)

func main() {
	input := openStorage(types.DirectoryPath(getEnv("THE_HASHER_INPUT")))
	output := openStorage(types.DirectoryPath(getEnv("THE_HASHER_OUTPUT")))
	extFilter := getEnv("THE_HASHER_FILTER")
	fmt := getEnv("THE_HASHER_OUTPUT_FORMAT")

	ctx := context.Background()
	l := hasher.StartInputListing(input, ctx)
	ftrd := hasher.StartFilter(l, extFilter)
	hshd := hasher.StartHasher(ftrd, input)
	o := hasher.StartOutputWriter(hshd, input, output, fmt)
	for k := range o {
		logrus.WithField("input", k.I).WithField("output", string(k.O)).Info("Item")
	}
}

func openStorage(d types.DirectoryPath) storage.Storage {
	ret, err := filesystem.NewFilesystemStore(d)
	if err != nil {
		logrus.WithError(err).Fatalf("Error while opening input")
	}
	return ret
}

func getEnv(key string) string {
	ret, ok := os.LookupEnv(key)
	if !ok {
		logrus.WithField("env", key).Fatal("Could not read environment variable")
	}
	logrus.WithField("env", key).WithField("value", ret).Info("Reading environment variable")
	return ret
}
