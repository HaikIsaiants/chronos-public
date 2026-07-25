package coordinator

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/HaikIsaiants/chronos/internal/observability"
	pb "go.etcd.io/raft/v3/raftpb"
	"go.opentelemetry.io/otel/trace"
)

func TestHTTPTransportPreservesTraceContext(t *testing.T) {
	headers := make(chan string, 1)
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		headers <- request.Header.Get("traceparent")
		writer.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()
	observer, err := observability.New(context.Background(), observability.Config{InstanceID: "1"})
	if err != nil {
		t.Fatal(err)
	}
	defer observer.Close(context.Background())
	reports := make(chan transportReport, 1)
	transport, err := newHTTPTransport(1, map[uint64]string{1: server.URL, 2: server.URL, 3: server.URL}, reports, observer)
	if err != nil {
		t.Fatal(err)
	}
	lifecycle, stop := context.WithCancel(context.Background())
	defer stop()
	transport.start(lifecycle)
	traceID := trace.TraceID{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}
	spanID := trace.SpanID{1, 2, 3, 4, 5, 6, 7, 8}
	parent, cancel := context.WithCancel(trace.ContextWithSpanContext(context.Background(), trace.NewSpanContext(trace.SpanContextConfig{
		TraceID: traceID, SpanID: spanID, TraceFlags: trace.FlagsSampled,
	})))
	cancel()
	from, to, kind := uint64(1), uint64(2), pb.MsgApp
	if err := transport.SendContext(parent, &pb.Message{From: &from, To: &to, Type: &kind}); err != nil {
		t.Fatal(err)
	}
	select {
	case header := <-headers:
		parts := strings.Split(header, "-")
		if len(parts) != 4 || parts[1] != traceID.String() {
			t.Fatalf("unexpected traceparent %q", header)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("transport did not deliver message")
	}
}
