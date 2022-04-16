package main

// [ ] Read config file for credentials, etc.
// [x] List S3 objects matching a given prefix
// [x] Stream a specific S3 object
// [ ] Integrate with autocompletion
// [ ] Handle HTTP/S
// [ ] Handle local files
// [ ] Any other backends?
// [ ] Tests!!

// [ ] Where's the AWS SDK golang reference?

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"net/url"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

func s3_split(rawUrl string) (bucket, key string) {
	parsedUrl, err := url.Parse(rawUrl)
	if err != nil {
		log.Fatal(err)
	}
	
	if parsedUrl.Scheme != "s3" {
		log.Fatalf("not an S3 url: %s", rawUrl)
	}

	bucket = parsedUrl.Host
	key = strings.TrimLeft(parsedUrl.Path, "/")
	return
}

func s3_cat(url string) {
	// TODO: use special configuration for the prefix
	bucket, key := s3_split(url)
	// log.Printf("bucket: %s key: %s", bucket, key)

	cfg, err := config.LoadDefaultConfig(context.TODO())
	if err != nil {
		log.Fatal(err)
	}

	client := s3.NewFromConfig(cfg)
	params := &s3.GetObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)}
	response, err := client.GetObject(context.TODO(), params)
	if err != nil {
		log.Fatal(err)
	}

	defer response.Body.Close()
	const bufsize = 1024768
	buffer := make([]byte, bufsize)
	for true {
		numBytes, err := response.Body.Read(buffer)
		if numBytes > 0 {
			fmt.Printf("%s", buffer[:numBytes])
		}
		if err == io.EOF {
			break
		} else if err != nil {
			log.Fatal(err)
		}
	}
}

func s3_list(prefix string) (candidates []string) {
	bucket, prefix := s3_split(prefix)

	cfg, err := config.LoadDefaultConfig(context.TODO())
	if err != nil {
		log.Fatal(err)
	}

	client := s3.NewFromConfig(cfg)
	params := &s3.ListObjectsInput{
		Bucket: aws.String(bucket),
		Prefix: aws.String(prefix),
		Delimiter: aws.String("/"),
	}

	objects := []string{}
	response, err := client.ListObjects(context.TODO(), params)
	if err != nil {
		log.Fatal(err)
	}
	// TODO: pagination?  Is it really worth it?
	for _, cp := range response.CommonPrefixes {
		objects = append(objects, *cp.Prefix)
	}
	for _, obj := range response.Contents {
		objects = append(objects, *obj.Key)
	}
	return objects
}

func main() {
	flag.Parse()

	protocol := "s3"
	if protocol == "s3" {
		objects := s3_list(flag.Args()[0])
		for _, thing := range objects {
			fmt.Println(thing)
		}
	}
}
