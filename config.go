package config

import (
	"encoding/json"
	"errors"
	"io/ioutil"
	"os"
	"path/filepath"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
)

//Config holds config values
type Config struct {
	Values map[string](map[string]string) // will hold  config values
}

//Source define a config source
type Source struct {
	URL      string //for direct download
	S3Bucket string //for S3 data source
	S3Path   string
	Path     string // for local imports
}

func (s *Config) getValuesFromS3(bucket string, path string) (map[string]string, error) {
	conf, err := getContentFromS3(bucket, path)
	if err != nil {
		return nil, err
	}
	result := map[string]string{}
	if err := json.Unmarshal(conf, &result); err != nil {
		return nil, errors.New(err.Error() + " in " + string(conf))
	}
	return result, err
}

//AddValues allows to add config values to a map
func (s *Config) AddValues(key string, source *Source) error {
	if source.S3Bucket != "" {
		//S3 config source
		result, err := s.getValuesFromS3(source.S3Bucket, source.Path)
		if err != nil {
			return err
		}
		s.initSource(key)
		s.Values[key] = result
		return nil
	}
	return errors.New("Unkwown data source")
}

func (s *Config) initSource(key string) {
	if s.Values == nil {
		s.Values = make(map[string]map[string]string)
	}
	if s.Values[key] == nil {
		s.Values[key] = make(map[string]string)
	}
}

//downloadFromS3 will get the file from the given bucket and path and will return the complete path of downloaded file
//or an error
func downloadFromS3(bucketName string, itemName string) (string, error) {

	sess, err := session.NewSession(&aws.Config{Region: aws.String("eu-west-3")})
	if err != nil {
		return "", err
	}

	extension := filepath.Ext(itemName)

	filename := "/tmp/" + RandomHex(30) + extension
	file, err := os.Create(filename)
	if err != nil {
		return "", err
	}

	defer file.Close()

	downloader := s3manager.NewDownloader(sess)
	_, err = downloader.Download(file,
		&s3.GetObjectInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(itemName),
		})
	if err != nil {
		return "", err
	}
	return filename, nil
}

/*getContentFromS3 will download the conf file from S3 and return the content as a file*/
func getContentFromS3(bucket string, bucketFileConf string) ([]byte, error) {
	//download file from S3
	filename, err := downloadFromS3(bucket, bucketFileConf)
	if err != nil {
		return nil, err
	}
	defer os.Remove(filename)
	return ioutil.ReadFile(filename)
}
