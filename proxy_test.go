package proxify

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/projectdiscovery/martian/v3"
	proxifylogger "github.com/projectdiscovery/proxify/pkg/logger"
	"github.com/projectdiscovery/proxify/pkg/logger/elastic"
	"github.com/projectdiscovery/proxify/pkg/logger/kafka"
)

func TestModifyRequestPreservesContentLengthAndBody(t *testing.T) {
	const body = "username=admin&password=secret"
	type receivedRequest struct {
		body             []byte
		contentLength    int64
		transferEncoding []string
		err              error
	}
	received := make(chan receivedRequest, 1)
	upstream := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		data, err := io.ReadAll(request.Body)
		received <- receivedRequest{
			body:             data,
			contentLength:    request.ContentLength,
			transferEncoding: request.TransferEncoding,
			err:              err,
		}
		response.WriteHeader(http.StatusNoContent)
	}))
	t.Cleanup(upstream.Close)

	request, err := http.NewRequest(http.MethodPost, upstream.URL+"/login", strings.NewReader(body))
	if err != nil {
		t.Fatalf("create request: %v", err)
	}
	originalContentLength := request.ContentLength
	_, removeContext, err := martian.TestContext(request, nil, nil)
	if err != nil {
		t.Fatalf("create proxy request context: %v", err)
	}
	t.Cleanup(removeContext)
	requestLogger := proxifylogger.NewLogger(&proxifylogger.OptionsLogger{
		Elastic: &elastic.Options{},
		Kafka:   &kafka.Options{},
	})
	t.Cleanup(requestLogger.Close)
	proxy := &Proxy{
		options: &Options{},
		logger:  requestLogger,
	}

	if err := proxy.ModifyRequest(request); err != nil {
		t.Fatalf("modify request: %v", err)
	}
	response, err := http.DefaultTransport.RoundTrip(request)
	if err != nil {
		t.Fatalf("forward request: %v", err)
	}
	if err := response.Body.Close(); err != nil {
		t.Fatalf("close response body: %v", err)
	}

	forwarded := <-received
	if forwarded.err != nil {
		t.Fatalf("upstream read body: %v", forwarded.err)
	}
	if string(forwarded.body) != body {
		t.Fatalf("forwarded body = %q, want %q", forwarded.body, body)
	}
	if request.ContentLength != originalContentLength {
		t.Fatalf("content length = %d, want %d", request.ContentLength, originalContentLength)
	}
	if forwarded.contentLength != originalContentLength {
		t.Fatalf("upstream content length = %d, want %d", forwarded.contentLength, originalContentLength)
	}
	if len(forwarded.transferEncoding) != 0 {
		t.Fatalf("upstream transfer encoding = %v, want none", forwarded.transferEncoding)
	}
}
