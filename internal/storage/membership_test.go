package storage

import (
	"errors"
	"fmt"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/HaikIsaiants/chronos/internal/core"
	"github.com/cockroachdb/pebble/v2"
	"go.etcd.io/raft/v3"
	pb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

func TestMembershipFilterGrowsWithoutFalseNegatives(t *testing.T) {
	filter := newMembershipFilter(nil)
	values := membershipValuesForShard(filter, 0, membershipShardInitialCapacity*4)
	filter.addAll(values)
	layers := filter.shards[0].layers.Load()
	if layers == nil || len(layers.values) != 3 {
		t.Fatalf("shard did not grow: %+v", layers)
	}
	for index, capacity := range []uint64{
		membershipShardInitialCapacity,
		membershipShardInitialCapacity * 2,
		membershipShardInitialCapacity * 4,
	} {
		layer := layers.values[index]
		if layer.capacity != capacity || len(layer.words) != int(capacity*membershipBitsPerID/64) {
			t.Fatalf("layer %d has capacity %d and %d words", index, layer.capacity, len(layer.words))
		}
	}
	for _, value := range values {
		if !filter.contains(value) {
			t.Fatalf("filter lost %s", value)
		}
	}
}

func TestMembershipFilterDistributesLazyShards(t *testing.T) {
	filter := newMembershipFilter(nil)
	values := make([]string, membershipShardCount*4)
	for index := range values {
		values[index] = fmt.Sprintf("distributed-%08d", index)
	}
	filter.addAll(values)
	if allocated := membershipAllocatedShards(filter); allocated < membershipShardCount*9/10 {
		t.Fatalf("only %d shards were allocated", allocated)
	}
	for index := range filter.shards {
		layers := filter.shards[index].layers.Load()
		if layers != nil && (len(layers.values) != 1 || layers.values[0].capacity != membershipShardInitialCapacity) {
			t.Fatalf("shard %d allocated unexpected layers", index)
		}
	}
	for _, value := range values {
		if !filter.contains(value) {
			t.Fatalf("filter lost %s", value)
		}
	}
}

func TestMembershipFilterLookupUsesOneShard(t *testing.T) {
	filter := newMembershipFilter(nil)
	present := membershipValuesForShard(filter, 0, 1)[0]
	missing := membershipValuesForShard(filter, 1, 1)[0]
	filter.addAll([]string{present})
	for index := range filter.shards[0].layers.Load().values[0].words {
		filter.shards[0].layers.Load().values[0].words[index].Store(^uint64(0))
	}
	if filter.shards[1].layers.Load() != nil || filter.contains(missing) {
		t.Fatal("lookup inspected another shard")
	}
}

func TestMembershipFilterConcurrentReaders(t *testing.T) {
	stable := make([]string, 1024)
	for index := range stable {
		stable[index] = fmt.Sprintf("stable-%08d", index)
	}
	filter := newMembershipFilter(stable)
	added := make([]string, 4096)
	for index := range added {
		added[index] = fmt.Sprintf("added-%08d", index)
	}
	var failed atomic.Bool
	stop := make(chan struct{})
	var readers sync.WaitGroup
	for range 8 {
		readers.Add(1)
		go func() {
			defer readers.Done()
			for {
				select {
				case <-stop:
					return
				default:
					for _, value := range stable {
						if !filter.contains(value) {
							failed.Store(true)
							return
						}
					}
				}
			}
		}()
	}
	filter.addAll(added)
	close(stop)
	readers.Wait()
	if failed.Load() {
		t.Fatal("concurrent reader observed a false negative")
	}
	for _, value := range added {
		if !filter.contains(value) {
			t.Fatalf("filter lost %s", value)
		}
	}
}

func TestMembershipFiltersUpdateOnlyAfterApplySync(t *testing.T) {
	armed := true
	config := Config{
		Directory: t.TempDir(), ClusterID: "membership-sync", NodeID: 1, Voters: []uint64{1, 2, 3},
		Failpoint: func(point Point) error {
			if armed && point == ApplyBeforeSync {
				return ErrInjectedCrash
			}
			return nil
		},
	}
	store, err := Open(config)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	persistApplicationEncodingRaft(t, store)
	entry := applicationEncodingFixture(t)[:1]
	before := store.membership.current.Load()
	if _, err := store.Apply(entry); !errors.Is(err, ErrInjectedCrash) {
		t.Fatalf("apply returned %v", err)
	}
	if store.membership.current.Load() != before {
		t.Fatal("failed apply replaced membership")
	}
	armed = false
	if _, err := store.Apply(entry); err != nil {
		t.Fatal(err)
	}
	membership := store.membership.current.Load()
	for _, requestID := range []string{"terminal-submit", "terminal-start"} {
		if !membership.receipts.contains(requestID) {
			t.Fatalf("receipt filter lost %s", requestID)
		}
	}
	if !membership.workflows.contains("terminal") {
		t.Fatal("workflow filter lost terminal")
	}
}

func TestMembershipFiltersRebuildStreamingOnOpen(t *testing.T) {
	directory := t.TempDir()
	config := Config{Directory: directory, ClusterID: "membership-open", NodeID: 1, Voters: []uint64{1, 2, 3}}
	store, err := Open(config)
	if err != nil {
		t.Fatal(err)
	}
	count := membershipShardCount * 2
	batch := store.db.NewBatchWithSize(count * 96)
	for index := range count {
		requestID := fmt.Sprintf("request-%08d", index)
		workflowID := fmt.Sprintf("workflow-%08d", index)
		if err := batch.Set(segmentedKey(membershipReceiptPrefix, requestID), []byte{1}, nil); err != nil {
			t.Fatal(err)
		}
		if err := batch.Set(segmentedKey(membershipWorkflowPrefix, workflowID), []byte{1}, nil); err != nil {
			t.Fatal(err)
		}
	}
	if err := batch.Commit(pebble.Sync); err != nil {
		t.Fatal(err)
	}
	if err := batch.Close(); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	store, err = Open(config)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	membership := store.membership.current.Load()
	if membershipAllocatedShards(membership.receipts) < membershipShardCount*3/4 || membershipAllocatedShards(membership.workflows) < membershipShardCount*3/4 {
		t.Fatal("reopened filters did not distribute IDs")
	}
	for index := range count {
		requestID := fmt.Sprintf("request-%08d", index)
		workflowID := fmt.Sprintf("workflow-%08d", index)
		if !membership.receipts.contains(requestID) || !membership.workflows.contains(workflowID) {
			t.Fatalf("reopened filters lost index %d", index)
		}
	}
}

func membershipValuesForShard(filter *membershipFilter, shard uint64, count int) []string {
	values := make([]string, 0, count)
	for index := 0; len(values) < count; index++ {
		value := fmt.Sprintf("shard-%04d-%08d", shard, index)
		if membershipShardIndex(filter.hash(value)) == shard {
			values = append(values, value)
		}
	}
	return values
}

func membershipAllocatedShards(filter *membershipFilter) int {
	count := 0
	for index := range filter.shards {
		if filter.shards[index].layers.Load() != nil {
			count++
		}
	}
	return count
}

func TestEngineMembershipMissesBypassDurableLoaders(t *testing.T) {
	store, err := Open(Config{Directory: t.TempDir(), ClusterID: "membership-miss", NodeID: 1, Voters: []uint64{1, 2, 3}})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	receiptSkips := store.membership.receiptSkips.Load()
	workflowSkips := store.membership.workflowSkips.Load()
	if _, exists, err := store.loadEngineRequest("missing-request"); err != nil || exists {
		t.Fatalf("request miss returned %t %v", exists, err)
	}
	if _, exists, err := store.loadEngineWorkflow("missing-workflow"); err != nil || exists {
		t.Fatalf("workflow miss returned %t %v", exists, err)
	}
	if store.membership.receiptSkips.Load() != receiptSkips+1 || store.membership.workflowSkips.Load() != workflowSkips+1 {
		t.Fatal("engine misses did not bypass durable loaders")
	}
	if _, exists, err := store.Receipt("public-miss"); err != nil || exists {
		t.Fatalf("public miss returned %t %v", exists, err)
	}
	if store.membership.receiptSkips.Load() != receiptSkips+1 {
		t.Fatal("public receipt used the engine filter")
	}
	if _, exists := store.State("public-workflow-miss"); exists {
		t.Fatal("public workflow miss exists")
	}
	if store.membership.workflowSkips.Load() != workflowSkips+1 {
		t.Fatal("public state used the engine filter")
	}
}

func TestSnapshotInstallReplacesMembershipFilters(t *testing.T) {
	source, err := Open(Config{Directory: t.TempDir(), ClusterID: "membership-snapshot", NodeID: 1, Voters: []uint64{1, 2, 3}})
	if err != nil {
		t.Fatal(err)
	}
	persistApplicationEncodingRaft(t, source)
	if _, err := source.Apply(applicationEncodingFixture(t)); err != nil {
		t.Fatal(err)
	}
	snapshot, err := source.CreateSnapshot()
	if err != nil {
		t.Fatal(err)
	}
	expected := source.Engine().Snapshot()
	if err := source.Close(); err != nil {
		t.Fatal(err)
	}
	destination, err := Open(Config{Directory: t.TempDir(), ClusterID: "membership-snapshot", NodeID: 2, Voters: []uint64{1, 2, 3}})
	if err != nil {
		t.Fatal(err)
	}
	defer destination.Close()
	before := destination.membership.current.Load()
	hard := &pb.HardState{Term: proto.Uint64(snapshot.GetMetadata().GetTerm()), Commit: proto.Uint64(snapshot.GetMetadata().GetIndex())}
	if err := destination.SaveReady(raft.Ready{HardState: hard, Snapshot: snapshot}); err != nil {
		t.Fatal(err)
	}
	if err := destination.InstallSnapshot(snapshot); err != nil {
		t.Fatal(err)
	}
	membership := destination.membership.current.Load()
	if membership == before {
		t.Fatal("snapshot retained the previous membership")
	}
	for _, request := range expected.Requests {
		if !membership.receipts.contains(request.RequestID) {
			t.Fatalf("snapshot receipt filter lost %s", request.RequestID)
		}
	}
	for _, workflow := range expected.Workflows {
		if !membership.workflows.contains(workflow.ID) {
			t.Fatalf("snapshot workflow filter lost %s", workflow.ID)
		}
	}
	if actual := destination.Engine().Snapshot(); !reflect.DeepEqual(actual, expected) {
		t.Fatal("snapshot membership changed engine state")
	}
	if receipt, exists, err := destination.loadEngineRequest("active-submit"); err != nil || !exists || receipt.RequestID != "active-submit" {
		t.Fatalf("snapshot receipt loader returned %+v %t %v", receipt, exists, err)
	}
	if state, exists, err := destination.loadEngineWorkflow("terminal"); err != nil || !exists || state.ID != "terminal" {
		t.Fatalf("snapshot workflow loader returned %+v %t %v", state, exists, err)
	}
}

func TestMembershipFilterEngineSubmissionMisses(t *testing.T) {
	store, err := Open(Config{Directory: t.TempDir(), ClusterID: "membership-engine", NodeID: 1, Voters: []uint64{1, 2, 3}})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	definition := core.WorkflowDefinition{
		Name: "membership", Namespace: "test",
		Tasks: []core.TaskDefinition{{ID: "task", Type: "work", Retry: core.RetryPolicy{MaxAttempts: 1, BackoffMultiplier: 1}}},
	}
	engine := store.Engine()
	if _, _, duplicate, err := engine.Prepare(core.Command{Kind: core.CommandSubmit, RequestID: "missing-request", WorkflowID: "missing-workflow", Definition: &definition}); err != nil || duplicate {
		t.Fatalf("prepare returned %v %t", err, duplicate)
	}
	if store.membership.receiptSkips.Load() == 0 || store.membership.workflowSkips.Load() == 0 {
		t.Fatal("engine did not use membership loaders")
	}
}
