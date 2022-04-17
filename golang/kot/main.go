package main

// [ ] Read config file for credentials, etc.
// [x] List S3 objects matching a given prefix
// [x] Stream a specific S3 object
// [x] Integrate with autocompletion
// [ ] Handle HTTP/S
// [ ] Handle local files
// [ ] Any other backends?
// [ ] Tests!!
// [ ] GNU cat-compatible command-line flags
// [ ] Proper packaging
// [ ] CI to build binaries for MacOS, Windows and Linux

// [ ] Where's the AWS SDK golang reference?
// [ ] How to package this thing without having to build separate binaries for kot, kedit, etc?

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
	"github.com/posener/complete/v2"
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

func s3_list(prefix string, silent bool) (candidates []string) {
	if prefix == "" {
		return candidates
	}

	bucket, prefix := s3_split(prefix)

	cfg, err := config.LoadDefaultConfig(context.TODO())
	if err != nil && silent {
		return candidates
	} else if err != nil {
		log.Fatal(err)
	}

	client := s3.NewFromConfig(cfg)

	//
	// Attempt bucket name autocompletion
	//
	if prefix == "" {
		listBucketsParams := &s3.ListBucketsInput{}
		response, err := client.ListBuckets(context.TODO(), listBucketsParams)
		if err != nil && silent {
			return candidates
		} else if err != nil {
			log.Fatal(err)
		}

		matchingBuckets := []string{}
		for _, b := range(response.Buckets) {
			if strings.HasPrefix(*b.Name, bucket) {
				matchingBuckets = append(matchingBuckets, *b.Name)
			}
		}

		if len(matchingBuckets) == 1 {
			bucket = matchingBuckets[0]
			prefix = ""
		} else {
			for _, b := range matchingBuckets {
				candidates = append(candidates, fmt.Sprintf("//%s", b))
			}
			return candidates
		}
	}

	//
	// Drill down as far as possible
	//
	for true {
		// log.Printf("prefix: %s", prefix)
		listObjectsParams := &s3.ListObjectsInput{
			Bucket: aws.String(bucket),
			Prefix: aws.String(prefix),
			Delimiter: aws.String("/"),
		}

		response, err := client.ListObjects(context.TODO(), listObjectsParams)
		if err != nil && silent {
			return candidates
		} else if err != nil {
			log.Fatal(err)
		}

		if len(response.CommonPrefixes) == 1 && len(response.Contents) == 0 {
			prefix = *response.CommonPrefixes[0].Prefix
			continue
		}

		// TODO: pagination?  Is it really worth it?
		// FIXME: why _must_ we include the //bucket, but exclude the s3: part?
		// Is colon some sort of special character for the autocompletion engine?

		for _, cp := range response.CommonPrefixes {
			fullUrl := fmt.Sprintf("//%s/%s", bucket, *cp.Prefix)
			candidates = append(candidates, fullUrl)
		}

		for _, obj := range response.Contents {
			fullUrl := fmt.Sprintf("//%s/%s", bucket, *obj.Key)
			candidates = append(candidates, fullUrl)
		}

		break
	}

	return candidates
}

type myPredictorType int

func (mpt myPredictorType) Predict(prefix string) (candidates []string) {
	parsedUrl, err := url.Parse(prefix)
	if err != nil {
		// log.Fatal(err)
		// Do nothing here
	}
	if parsedUrl.Scheme == "s3" {
		return s3_list(prefix, true)
	}
	// log.Fatalf("predictor functionality for scheme %s not implemented yet", parsedUrl.Scheme)
	return candidates
}

func cat(rawUrl string) {
	parsedUrl, err := url.Parse(rawUrl)
	if err != nil {
		log.Fatal(err)
	}
	if parsedUrl.Scheme == "s3" {
		s3_cat(rawUrl)
	}
	log.Fatalf("cat functionality for scheme %s not implemented yet", parsedUrl.Scheme)
}

func main() {
	var testFlag = flag.Bool("test", false, "test the predictor")

	var myPredictor myPredictorType
	cmd := &complete.Command{
		Args: myPredictor,
	}
	cmd.Complete("kot")

	flag.Parse()

	if *testFlag {
		for _, thing := range myPredictor.Predict(flag.Args()[0]) {
			fmt.Println(thing)
		}
		return
	}

	for _, thing := range flag.Args() {
		s3_cat(thing)
	}
}
