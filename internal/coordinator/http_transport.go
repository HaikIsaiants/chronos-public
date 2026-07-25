package coordinator

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/HaikIsaiants/chronos/internal/observability"
	pb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

type transportReport struct {
	nodeID   uint64
	snapshot bool
	success  bool
}

type transportMessage struct {
	context context.Context
	message *pb.Message
}

type httpTransport struct {
	client         *http.Client
	snapshotClient *http.Client
	peers          map[uint64]string
	queues         map[uint64]chan transportMessage
	reports        chan<- transportReport
}

func newHTTPTransport(nodeID uint64, peers map[uint64]string, reports chan<- transportReport, observer *observability.Provider) (*httpTransport, error) {
	var base http.RoundTripper = http.DefaultTransport
	if observer != nil {
		base = observer.HTTPTransport(base)
	}
	transport := &httpTransport{
		client: &http.Client{Timeout: 2 * time.Second, Transport: base}, snapshotClient: &http.Client{Timeout: 10 * time.Minute, Transport: base},
		peers: make(map[uint64]string), queues: make(map[uint64]chan transportMessage), reports: reports,
	}
	for id, address := range peers {
		if id == nodeID {
			continue
		}
		if address == "" {
			return nil, fmt.Errorf("peer %d has no address", id)
		}
		transport.peers[id] = strings.TrimRight(address, "/")
		transport.queues[id] = make(chan transportMessage, 4096)
	}
	if len(transport.peers) != 2 {
		return nil, fmt.Errorf("three peer addresses are required")
	}
	return transport, nil
}

func (t *httpTransport) start(context context.Context) {
	// One sender per peer preserves raft message order
	for id, queue := range t.queues {
		go t.runPeer(context, id, queue)
	}
}

func (t *httpTransport) Send(message *pb.Message) error {
	return t.SendContext(context.Background(), message)
}

func (t *httpTransport) SendContext(ctx context.Context, message *pb.Message) error {
	queue, exists := t.queues[message.GetTo()]
	if !exists {
		return fmt.Errorf("peer %d is unknown", message.GetTo())
	}
	select {
	// carry trace values into transport-owned requests
	case queue <- transportMessage{context: context.WithoutCancel(ctx), message: message}:
		return nil
	default:
		return fmt.Errorf("peer %d queue is full", message.GetTo())
	}
}

func (t *httpTransport) runPeer(ctx context.Context, nodeID uint64, queue <-chan transportMessage) {
	for {
		select {
		case <-ctx.Done():
			return
		case queued := <-queue:
			delivery, cancel := context.WithCancel(queued.context)
			stop := context.AfterFunc(ctx, cancel)
			success := t.deliver(delivery, nodeID, queued.message) == nil
			stop()
			cancel()
			if !success || queued.message.GetType() == pb.MsgSnap {
				select {
				case t.reports <- transportReport{nodeID: nodeID, snapshot: queued.message.GetType() == pb.MsgSnap, success: success}:
				case <-ctx.Done():
					return
				}
			}
		}
	}
}

func (t *httpTransport) deliver(context context.Context, nodeID uint64, message *pb.Message) error {
	data, err := proto.Marshal(message)
	if err != nil {
		return err
	}
	request, err := http.NewRequestWithContext(context, http.MethodPost, t.peers[nodeID]+"/raft", bytes.NewReader(data))
	if err != nil {
		return err
	}
	request.Header.Set("Content-Type", "application/x-protobuf")
	client := t.client
	if message.GetType() == pb.MsgSnap {
		client = t.snapshotClient
	}
	response, err := client.Do(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	io.Copy(io.Discard, response.Body)
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return fmt.Errorf("peer %d returned %s", nodeID, response.Status)
	}
	return nil
}
