package backend

import (
	"fmt"
	"github.com/aliyun/aliyun-oss-go-sdk/oss"
	"github.com/meltwater/drone-cache/cache"
	"io"
	"log"
)

// ossBackend is an oss implementation of the Backend
type ossBackend struct {
	bucket     string
	acl        string
	encryption string
	client     *oss.Client
}

const (
	_CONNECTION_TIMEOUT = 5
	_READ_WRITE_TIMEOUT = 60
)

// newOss returns a new oss remote Backend implemented
func newOss(bucket, acl, encryption string, conf *oss.Config) cache.Backend {
	timeout := oss.Timeout(int64(_CONNECTION_TIMEOUT), int64(_READ_WRITE_TIMEOUT))
	client, err := oss.New(conf.Endpoint, conf.AccessKeyID, conf.AccessKeySecret, timeout)
	if err != nil {
		log.Fatal(err)
		return nil
	}
	return &ossBackend{
		bucket:     bucket,
		acl:        acl,
		encryption: encryption,
		client:     client,
	}
}

// Get returns an io.Reader for reading the contents of the file
func (c *ossBackend) Get(p string) (io.ReadCloser, error) {
	bk, err := c.client.Bucket(c.bucket)
	if err != nil {
		return nil, err
	}
	out, err := bk.GetObject(p)
	if err != nil {
		return nil, fmt.Errorf("get the object %v", err)
	}
	return out, nil
}

// Put uploads the contents of the io.ReadSeeker
func (c *ossBackend) Put(p string, src io.ReadSeeker) error {
	bk, err := c.client.Bucket(c.bucket)
	if err != nil {
		return err
	}
	var options []oss.Option
	if c.encryption != "" {
		options = append(options, oss.ServerSideEncryption(c.encryption))
	}
	if c.acl != "" {
		options = append(options, oss.ACL(oss.ACLType(c.acl)))
	}

	if err = bk.PutObject(p, src, options...); err != nil {
		return fmt.Errorf("put the object %v", err)
	}

	return nil
}
