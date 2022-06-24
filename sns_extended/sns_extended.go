package snsextended

import (
	"encoding/json"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/aws/aws-sdk-go/service/sns"
	"github.com/google/uuid"
)

type SnsExtended struct {
	config SnsExtendedConfiguration
	snsService     *sns.SNS
	s3Manager      *s3manager.Uploader
}

type SnsExtendedConfiguration struct {
	sizeThreshold uint
	bucket string
}

func New(sizeThreashold uint, bucket string) *SnsExtended {
	sess := session.Must(session.NewSession())
	snsService := sns.New(sess)
	s3Manager := s3manager.NewUploader(sess)

	return &SnsExtended{
		snsService: snsService,
		s3Manager:  s3Manager,
		config: SnsExtendedConfiguration{
			sizeThreashold,
			bucket,
		},
	}
}

func (client *SnsExtended) Publish(message string, topicArn string, messageAttributes map[string]*sns.MessageAttributeValue) {
	if len(message) > int(client.config.sizeThreshold) {
		id := uuid.New().String()

		client.s3Manager.Upload(&s3manager.UploadInput{
			Bucket: aws.String(client.config.bucket),
			Key:    aws.String(id),
			Body:   strings.NewReader(message),
		})

		snsMessage, _ := json.Marshal(struct {
			ThroughS3 bool
			Key       string
		}{
			ThroughS3: true,
			Key:       id,
		})

		client.snsService.Publish(&sns.PublishInput{
			Message:           aws.String(string(snsMessage)),
			TopicArn:          aws.String(topicArn),
			MessageAttributes: messageAttributes,
		})
	} else {
		client.snsService.Publish(&sns.PublishInput{
			Message:           aws.String(string(message)),
			TopicArn:          aws.String(topicArn),
			MessageAttributes: messageAttributes,
		})
	}
}
