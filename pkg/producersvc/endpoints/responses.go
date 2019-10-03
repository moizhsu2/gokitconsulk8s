package endpoints

import (
	"net/http"

	httptransport "github.com/go-kit/kit/transport/http"
)

var (
	_ httptransport.Headerer = (*SumResponse)(nil)

	_ httptransport.StatusCoder = (*SumResponse)(nil)

	_ httptransport.Headerer = (*ConcatResponse)(nil)

	_ httptransport.StatusCoder = (*ConcatResponse)(nil)

	_ httptransport.Headerer = (*ProduceResponse)(nil)

	_ httptransport.StatusCoder = (*ProduceResponse)(nil)
)

// SumResponse collects the response values for the Sum method.
type SumResponse struct {
	Rs  int64 `json:"rs"`
	Err error `json:"err"`
}

func (r SumResponse) StatusCode() int {
	return http.StatusOK // TBA
}

func (r SumResponse) Headers() http.Header {
	return http.Header{}
}

// ConcatResponse collects the response values for the Concat method.
type ConcatResponse struct {
	Rs  string `json:"rs"`
	Err error  `json:"err"`
}

func (r ConcatResponse) StatusCode() int {
	return http.StatusOK // TBA
}

func (r ConcatResponse) Headers() http.Header {
	return http.Header{}
}

// ProduceResponse .
type ProduceResponse struct {
	Rs  string `json:"rs"`
	Err error  `json:"err"`
}

func (r ProduceResponse) StatusCode() int {
	return http.StatusOK // TBA
}

func (r ProduceResponse) Headers() http.Header {
	return http.Header{}
}
