package hasher

import (
	"context"
	"io"
	"path/filepath"
	"strings"

	"github.com/maciejgaleja/gosimple/pkg/hash"
	"github.com/maciejgaleja/gosimple/pkg/storage"
	"github.com/maciejgaleja/gosimple/pkg/types"
	"github.com/sirupsen/logrus"
)

type Item struct {
	I storage.Key
	O storage.Key
	E types.FileExtension
	H hash.Sha512
}

func StartInputListing(s storage.Storage, ctx context.Context) <-chan storage.Key {
	ks := make(chan storage.Key)
	go func() {
		defer close(ks)
		l, err := s.List()
		if err != nil {
			logrus.WithContext(ctx).WithError(err).Error("Error while listing input")
			return
		}
		for _, k := range l {
			select {
			case <-ctx.Done():
				return
			case ks <- k:
				continue
			}
		}
		logrus.WithContext(ctx).Info("Finished listing input")
	}()
	return ks
}

func StartFilter(in <-chan storage.Key, extensions string) <-chan Item {
	es := strings.Split(extensions, ",")
	out := make(chan Item)
	go func() {
		defer close(out)
		for k := range in {
			ext := strings.ToLower(filepath.Ext(string(k))[1:])
			if contains(es, ext) {
				out <- Item{I: k, E: types.FileExtension(ext)}
			}
		}
	}()
	return out
}

func StartHasher(ks <-chan Item, input storage.Storage) <-chan Item {
	ret := make(chan Item)
	go func() {
		defer close(ret)
		for k := range ks {
			r, err := input.Reader(k.I)
			if err != nil {
				logrus.WithError(err).Fatalf("error while reading object from input")
			}
			k.H = hash.Hash(r)
			err = r.Close()
			if err != nil {
				logrus.WithError(err).Fatalf("error while reading object from input")
			}
			ret <- k
		}
	}()
	return ret
}

func StartOutputWriter(hks <-chan Item, input, output storage.Storage, fmt string) <-chan Item {
	ret := make(chan Item)
	go func() {
		defer close(ret)
		for k := range hks {
			k = formatOutputKey(k, fmt)
			if output.Exists(k.O) {
				continue
			} else {
				w, err := output.Create(k.O)
				if err != nil {
					logrus.WithError(err).Fatal("Error while writing output")
				}
				r, err := input.Reader(k.I)
				if err != nil {
					logrus.WithError(err).Fatal("Error while reading from input")
				}
				_, err = io.Copy(w, r)
				if err != nil {
					logrus.WithError(err).Fatal("Error while copying form input to output")
				}
			}
			ret <- k
		}
	}()
	return ret
}

func contains(s []string, e string) bool {
	for _, a := range s {
		if a == e {
			return true
		}
	}
	return false
}

func formatOutputKey(h Item, fmt string) Item {
	fmt = strings.ReplaceAll(fmt, "%e", string(h.E))
	fmt = strings.ReplaceAll(fmt, "%h", string(h.H))
	i := h
	i.O = storage.Key(fmt)
	return i
}
