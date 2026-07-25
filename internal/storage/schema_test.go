package storage

import (
	"encoding/binary"
	"errors"
	"testing"

	"github.com/cockroachdb/pebble/v2"
)

func TestOpenRejectsSchemaFive(t *testing.T) {
	config := Config{Directory: t.TempDir(), ClusterID: "schema-five", NodeID: 1, Voters: []uint64{1, 2, 3}}
	store, err := Open(config)
	if err != nil {
		t.Fatal(err)
	}
	var schema [4]byte
	binary.BigEndian.PutUint32(schema[:], SchemaVersion-1)
	if err := store.db.Set(schemaKey, schema[:], pebble.Sync); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	if reopened, err := Open(config); !errors.Is(err, ErrStoreVersion) {
		if err == nil {
			reopened.Close()
		}
		t.Fatalf("schema five returned %v", err)
	}
}
