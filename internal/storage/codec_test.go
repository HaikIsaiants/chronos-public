package storage

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
)

var jsonCodecBenchmarkBytes []byte

func TestJSONCodec(t *testing.T) {
	small := AppliedState{Index: 2, Term: 1}
	encoded, err := encodeJSON(small)
	if err != nil || !json.Valid(encoded) {
		t.Fatalf("small value was not plain JSON: %v", err)
	}
	var decoded AppliedState
	if err := decodeJSON(encoded, &decoded); err != nil || decoded != small {
		t.Fatalf("small value round trip failed: %+v %v", decoded, err)
	}
	large := map[string]string{"value": strings.Repeat("compressible", 4096)}
	encoded, err = encodeJSON(large)
	if err != nil || !bytes.HasPrefix(encoded, jsonSnappyMagic) {
		t.Fatalf("large value was not compressed: %v", err)
	}
	var restored map[string]string
	if err := decodeJSON(encoded, &restored); err != nil || restored["value"] != large["value"] {
		t.Fatalf("large value round trip failed: %v", err)
	}
	legacy, err := json.Marshal(large)
	if err != nil {
		t.Fatal(err)
	}
	if err := decodeJSON(legacy, &restored); err != nil || restored["value"] != large["value"] {
		t.Fatalf("legacy value failed: %v", err)
	}
	if err := decodeJSON(encoded[:len(encoded)-1], &restored); err == nil {
		t.Fatal("truncated compressed value decoded")
	}
	header := make([]byte, binary.MaxVarintLen64)
	length := binary.PutUvarint(header, maxJSONRecordBytes+1)
	oversized := append(append([]byte(nil), jsonSnappyMagic...), header[:length]...)
	if err := decodeJSON(oversized, &restored); err == nil {
		t.Fatal("oversized compressed value decoded")
	}
}

func TestSnapshotCodecAcceptsPayloadAboveRecordLimit(t *testing.T) {
	image := SnapshotImage{Version: SnapshotVersion, ClusterID: strings.Repeat("x", maxJSONRecordBytes)}
	payload, err := encodeJSON(image)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.HasPrefix(payload, jsonSnappyMagic) {
		t.Fatal("large payload was compressed")
	}
	encoded := make([]byte, 16+len(payload)+sha256.Size)
	copy(encoded, snapshotMagic)
	binary.BigEndian.PutUint64(encoded[8:16], uint64(len(payload)))
	copy(encoded[16:], payload)
	hash := sha256.Sum256(encoded[:16+len(payload)])
	copy(encoded[16+len(payload):], hash[:])
	decoded, err := decodeSnapshotImage(encoded)
	if err != nil {
		t.Fatal(err)
	}
	if len(decoded.ClusterID) != maxJSONRecordBytes {
		t.Fatalf("cluster id length = %d", len(decoded.ClusterID))
	}
}

func BenchmarkJSONCompressionCutoffRepeated(b *testing.B) {
	sizes := []int{128, 192, 255, 256, 257, 384, 512, 1024, 4096}
	// todo: add a low-repetition event corpus
	for _, size := range sizes {
		value := strings.Repeat("x", size-2)
		encoded, err := encodeJSON(value)
		if err != nil {
			b.Fatal(err)
		}
		b.Run(fmt.Sprintf("%d_bytes", size), func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(int64(size))
			for b.Loop() {
				data, err := encodeJSON(value)
				if err != nil {
					b.Fatal(err)
				}
				jsonCodecBenchmarkBytes = data
			}
			b.ReportMetric(float64(len(encoded)), "stored_B/op")
			b.ReportMetric(float64(len(encoded))/float64(size), "stored/raw")
		})
	}
}
