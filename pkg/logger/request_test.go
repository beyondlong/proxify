package logger

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestCloneRequestForLoggingStreamsIndependentBody(t *testing.T) {
	const body = "username=admin&password=secret"
	req := httptest.NewRequest(http.MethodPost, "http://example.com/login", strings.NewReader(body))
	originalContentLength := req.ContentLength

	logRequest := cloneRequestForLogging(req)
	loggedBody := make(chan struct {
		body []byte
		err  error
	}, 1)
	go func() {
		data, err := io.ReadAll(logRequest.Body)
		loggedBody <- struct {
			body []byte
			err  error
		}{body: data, err: err}
	}()

	forwardedBody, err := io.ReadAll(req.Body)
	if err != nil {
		t.Fatalf("read forwarding body: %v", err)
	}
	if err := req.Body.Close(); err != nil {
		t.Fatalf("close forwarding body: %v", err)
	}

	logged := <-loggedBody
	if logged.err != nil {
		t.Fatalf("read logging body: %v", logged.err)
	}
	if string(forwardedBody) != body {
		t.Fatalf("forwarded body = %q, want %q", forwardedBody, body)
	}
	if string(logged.body) != body {
		t.Fatalf("logged body = %q, want %q", logged.body, body)
	}
	if req.ContentLength != originalContentLength {
		t.Fatalf("forwarding content length = %d, want %d", req.ContentLength, originalContentLength)
	}
	if logRequest.ContentLength != originalContentLength {
		t.Fatalf("logging content length = %d, want %d", logRequest.ContentLength, originalContentLength)
	}
}

func TestCloneRequestForLoggingPreservesUnknownLength(t *testing.T) {
	const body = "streamed-body"
	req := httptest.NewRequest(http.MethodPost, "http://example.com/upload", strings.NewReader(body))
	req.ContentLength = -1

	logRequest := cloneRequestForLogging(req)
	loggedBody := make(chan []byte, 1)
	go func() {
		data, _ := io.ReadAll(logRequest.Body)
		loggedBody <- data
	}()

	forwardedBody, err := io.ReadAll(req.Body)
	if err != nil {
		t.Fatalf("read forwarding body: %v", err)
	}
	if err := req.Body.Close(); err != nil {
		t.Fatalf("close forwarding body: %v", err)
	}

	if string(forwardedBody) != body {
		t.Fatalf("forwarded body = %q, want %q", forwardedBody, body)
	}
	if string(<-loggedBody) != body {
		t.Fatalf("logged body does not match %q", body)
	}
	if req.ContentLength != -1 || logRequest.ContentLength != -1 {
		t.Fatalf("unknown content length changed: forwarding=%d logging=%d", req.ContentLength, logRequest.ContentLength)
	}
}

func TestCloneRequestForLoggingDoesNotFailForwardingWhenLoggerStops(t *testing.T) {
	const body = "forward-even-if-logging-stops"
	req := httptest.NewRequest(http.MethodPost, "http://example.com/upload", strings.NewReader(body))

	logRequest := cloneRequestForLogging(req)
	if err := logRequest.Body.Close(); err != nil {
		t.Fatalf("close logging body: %v", err)
	}

	forwardedBody, err := io.ReadAll(req.Body)
	if err != nil {
		t.Fatalf("read forwarding body after logger close: %v", err)
	}
	if string(forwardedBody) != body {
		t.Fatalf("forwarded body = %q, want %q", forwardedBody, body)
	}
}
