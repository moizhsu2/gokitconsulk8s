package service

import (
	"context"
	"fmt"

	"github.com/Shopify/sarama"
	"github.com/go-kit/kit/log"
)

// Middleware describes a service (as opposed to endpoint) middleware.
type Middleware func(ProducersvcService) ProducersvcService

// Service describes a service that adds things together
// Implement yor service methods methods.
// e.x: Foo(ctx context.Context, s string)(rs string, err error)
type ProducersvcService interface {
	Sum(ctx context.Context, a int64, b int64) (rs int64, err error)
	Concat(ctx context.Context, a string, b string) (rs string, err error)
	Produce(ctx context.Context, topic, msg string) (rs string, err error)
}

// the concrete implementation of service interface
type stubProducersvcService struct {
	logger log.Logger `json:"logger"`
	kaf    sarama.SyncProducer
}

// New return a new instance of the service.
// If you want to add service middleware this is the place to put them.
func New(logger log.Logger) (s ProducersvcService) {
	kaf := newDataProducer([]string{"my-kafka:9092"}, logger)
	var svc ProducersvcService
	{
		svc = &stubProducersvcService{
			logger: logger,
			kaf:    kaf,
		}
		svc = LoggingMiddleware(logger)(svc)
	}
	return svc
}

func newDataProducer(brokerList []string, logger log.Logger) sarama.SyncProducer {
	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Retry.Max = 10
	config.Producer.Return.Successes = true

	producer, err := sarama.NewSyncProducer(brokerList, config)
	if err != nil {
		logger.Log("Failed to start Sarama producer:", err.Error())
	}

	return producer
}

// Implement the business logic of Sum
func (ad *stubProducersvcService) Sum(ctx context.Context, a int64, b int64) (rs int64, err error) {
	return a + b, err
}

// Implement the business logic of Concat
func (ad *stubProducersvcService) Concat(ctx context.Context, a string, b string) (rs string, err error) {
	return a + b, err
}

func (ad *stubProducersvcService) Produce(ctx context.Context, topic, msg string) (rs string, err error) {
	//msa := sarama.
	m := &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.StringEncoder(msg),
	}
	partition, _, err := ad.kaf.SendMessage(m)
	fmt.Printf("Topic %s Produced! \n Partition = %d content = %s .", topic, partition, msg)
	return "ok", err
}
