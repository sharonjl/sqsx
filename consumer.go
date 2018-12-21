package sqsx

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/sqs"
	"sync/atomic"
	"time"
)

type ConsumeHandler interface {
	// Handle is called when a message is received when polling SQS.
	Handle(message *sqs.Message, deadline ExtendTimeout) error
	
	// Error handles any errors returned from Handle(...). The param
	// messageHandled is true if Handle(...) returned an error, false if
	// the message was handled but an error was encountered performing 
	// queue operations. (extending deadline, or delete)
	Error(message *sqs.Message, messageHandled bool, err error)
}

type ExtendTimeout func(message *sqs.Message, timeout time.Duration) error

type ConsumerConfig struct {
	MaxWorkers int

	PollTimeout time.Duration

	Timeout time.Duration
}

type Consumer interface {
	Start(handler ConsumeHandler) error
	Stop()
}

type consumer struct {
	queueName, queueURL string
	config              *ConsumerConfig
	svc                 Service
	stop                chan chan bool

	consumeFn       func(m *sqs.Message, handler ConsumeHandler)
	extendTimeoutFn ExtendTimeout
}

func (c *consumer) Start(handler ConsumeHandler) error {
	var stopped chan bool
	free := int32(c.config.MaxWorkers)

	for {
		select {
		case stopped = <-c.stop:
			for {
				if atomic.LoadInt32(&free) == int32(c.config.MaxWorkers) {
					stopped <- true
					return nil
				}
			}
		default:
			maxMessages := atomic.LoadInt32(&free)
			if maxMessages == 0 {
				continue
			}
			if SQSMaxBatchSize < maxMessages {
				maxMessages = SQSMaxBatchSize
			}

			result, err := c.svc.ReceiveMessage(&sqs.ReceiveMessageInput{
				QueueUrl:            aws.String(c.queueURL),
				MaxNumberOfMessages: aws.Int64(int64(maxMessages)),
				WaitTimeSeconds:     aws.Int64(int64(c.config.PollTimeout.Seconds())),
				VisibilityTimeout:   aws.Int64(int64(c.config.Timeout.Seconds())),
			})
			if err != nil {
				return NewErr(err, "unable to receive message(s) from queue %q", c.queueName)
			}

			if len(result.Messages) <= 0 {
				continue
			}
			for _, m := range result.Messages {
				atomic.AddInt32(&free, -1)
				go func(m *sqs.Message) {
					c.consumeFn(m, handler)
					atomic.AddInt32(&free, 1)
				}(m)
			}
		}
	}
}

func (c *consumer) consume(m *sqs.Message, handler ConsumeHandler) {
	// Handle message
	if err := handler.Handle(m, c.extendTimeoutFn); err != nil {
		handler.Error(m, false, err)
		return
	}

	// Delete message from SQS
	inp := &sqs.DeleteMessageInput{
		QueueUrl:      aws.String(c.queueURL),
		ReceiptHandle: m.ReceiptHandle,
	}
	if _, err := c.svc.DeleteMessage(inp); err != nil {
		// The message was processed but we couldn't
		// delete the message from queue.
		handler.Error(m, true, err)
		return
	}
}

func (c *consumer) extendTimeout(message *sqs.Message, timeout time.Duration) error {
	inp := &sqs.ChangeMessageVisibilityInput{
		ReceiptHandle:     message.ReceiptHandle,
		QueueUrl:          aws.String(c.queueURL),
		VisibilityTimeout: aws.Int64(int64(timeout.Seconds())),
	}
	_, err := c.svc.ChangeMessageVisibility(inp)
	if err != nil {
		return NewErr(err, "could not extend deadline for message %q", aws.StringValue(message.MessageId))
	}
	return nil
}

func (c *consumer) Stop() {
	done := make(chan bool)
	c.stop <- done
	<-done
}

func NewConsumer(queueName string, svc Service, config ...*ConsumerConfig) (Consumer, error) {
	if queueName == "" {
		return nil, ErrQueueDoesNotExist
	}
	resp, err := svc.GetQueueUrl(&sqs.GetQueueUrlInput{QueueName: aws.String(queueName)})
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok && aerr.Code() == sqs.ErrCodeQueueDoesNotExist {
			return nil, ErrQueueDoesNotExist
		}
		return nil, NewErr(err, "could not get outbox URL")
	}

	c := &consumer{
		queueName: queueName,
		queueURL:  aws.StringValue(resp.QueueUrl),
		svc:       svc,
		stop:      make(chan chan bool),
		config:    &ConsumerConfig{PollTimeout: SQSMaxPollTimeout, MaxWorkers: 1},
	}
	for _, cfg := range config {
		if cfg == nil {
			continue
		}
		if cfg.PollTimeout < SQSMaxPollTimeout {
			c.config.PollTimeout = cfg.PollTimeout
		}
		if cfg.MaxWorkers > 0 {
			c.config.MaxWorkers = cfg.MaxWorkers
		}
		c.config.Timeout = cfg.Timeout
	}
	c.consumeFn = c.consume
	c.extendTimeoutFn = c.extendTimeout
	return c, nil
}
