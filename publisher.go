package sqsx

import (
	"encoding/json"
	"errors"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/satori/go.uuid"
	"time"
)

const (
	sqsMaxBatchSize = 10
)

var (
	ErrInvalidMessage    = errors.New("invalid message")
	ErrQueueDoesNotExist = errors.New("queue does not exist")
)

type Publisher interface {
	Stop()
	Start()

	Publish(msg interface{}, config ...*MessageConfig) error
}

type Message struct {
	Data interface{} `json:"data"`
}

type envelope struct {
	id     string
	m      Message
	status chan error
	delay  time.Duration
}

type pubImpl struct {
	queueName, queueURL string
	config              *PublisherConfig
	svc                 Service
	stop                chan chan bool
	outbox              chan envelope

	// Functions
	publishBatchFn func([]*envelope)
	jsonMarshalFn  func(v interface{}) ([]byte, error)
}

func (p pubImpl) Stop() {
	wait := make(chan bool)
	p.stop <- wait
	<-wait
}

func (p pubImpl) Start() {
	tout := time.NewTicker(p.config.BatchWindow)
	var ee []*envelope
	for {
		if len(ee) == sqsMaxBatchSize {
			p.publishBatchFn(ee)
			ee = nil
		}

		select {
		case <-tout.C:
			if len(ee) == 0 {
				continue
			}
			p.publishBatchFn(ee)
			ee = nil
		case e := <-p.outbox:
			ee = append(ee, &e)
		case w := <-p.stop:
			tout.Stop()
			if len(ee) > 0 {
				p.publishBatchFn(ee)
				ee = nil
			}
			w <- true
		}
	}
}

func (p pubImpl) publishBatch(ee []*envelope) {
	var (
		bee    []*sqs.SendMessageBatchRequestEntry
		envMap map[string]*envelope
	)

	batchID := uuid.NewV4().String()
	envMap = make(map[string]*envelope)
	for _, e := range ee {
		b, err := p.jsonMarshalFn(e.m)
		if err != nil {
			// This is unlikely to happen, here in case it does.
			e.status <- errorf(err, "could not publish message")
			continue
		}
		be := &sqs.SendMessageBatchRequestEntry{
			Id:             aws.String(e.id),
			MessageGroupId: aws.String(batchID),
			DelaySeconds:   aws.Int64(int64(e.delay.Seconds())),
			MessageBody:    aws.String(string(b)),
		}
		bee = append(bee, be)

		// Keep track of id to envelope assignment.
		envMap[e.id] = e
	}

	if len(bee) == 0 {
		return
	}

	resp, err := p.svc.SendMessageBatch(&sqs.SendMessageBatchInput{QueueUrl: aws.String(p.queueURL), Entries: bee})
	if err != nil {
		for _, en := range bee {
			envMap[aws.StringValue(en.Id)].status <- errorf(err, "could not publish message")
		}
		return
	}

	// Relay success
	for _, f := range resp.Successful {
		envMap[aws.StringValue(f.Id)].status <- nil
	}

	// Relay failure results back
	for _, f := range resp.Failed {
		en := envMap[aws.StringValue(f.Id)]
		en.status <- errorf(nil, "could not publish message\nMessage=%q\nCode=%q", aws.StringValue(f.Message), aws.StringValue(f.Code))
	}
}

func (p pubImpl) Publish(msg interface{}, config ...*MessageConfig) error {
	if msg == nil {
		return ErrInvalidMessage
	}
	m := Message{Data: msg}
	e := envelope{id: uuid.NewV4().String(), m: m, status: make(chan error)}
	for _, c := range config {
		if c == nil {
			continue
		}
		e.delay = c.Delay
	}
	p.outbox <- e
	return <-e.status
}

func NewPublisher(queueName string, svc Service, config ...*PublisherConfig) (Publisher, error) {
	if queueName == "" {
		return nil, ErrQueueDoesNotExist
	}
	resp, err := svc.GetQueueUrl(&sqs.GetQueueUrlInput{QueueName: aws.String(queueName)})
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok && aerr.Code() == sqs.ErrCodeQueueDoesNotExist {
			return nil, ErrQueueDoesNotExist
		}
		return nil, errorf(err, "could not get outbox URL")
	}

	p := &pubImpl{
		queueName: queueName,
		queueURL:  aws.StringValue(resp.QueueUrl),
		svc:       svc,
		outbox:    make(chan envelope),
		stop:      make(chan chan bool),
		config:    &PublisherConfig{BatchWindow: time.Millisecond * 7},
	}
	for _, c := range config {
		if c == nil {
			continue
		}
		p.config.BatchWindow = c.BatchWindow
	}
	p.jsonMarshalFn = json.Marshal
	p.publishBatchFn = p.publishBatch
	return p, nil
}
