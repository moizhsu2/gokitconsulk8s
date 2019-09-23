package service

import (
	"context"

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
}

// the concrete implementation of service interface
type stubProducersvcService struct {
	logger log.Logger `json:"logger"`
}

// New return a new instance of the service.
// If you want to add service middleware this is the place to put them.
func New(logger log.Logger) (s ProducersvcService) {
	var svc ProducersvcService
	{
		svc = &stubProducersvcService{logger: logger}
		svc = LoggingMiddleware(logger)(svc)
	}
	return svc
}

// Implement the business logic of Sum
func (ad *stubProducersvcService) Sum(ctx context.Context, a int64, b int64) (rs int64, err error) {
	return a + b, err
}

// Implement the business logic of Concat
func (ad *stubProducersvcService) Concat(ctx context.Context, a string, b string) (rs string, err error) {
	return a + b, err
}
