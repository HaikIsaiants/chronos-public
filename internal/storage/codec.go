package storage

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"fmt"

	"github.com/golang/snappy"
	"google.golang.org/protobuf/proto"
)

var schemaKey = []byte{1, 1}
var hardStateKey = []byte{1, 2}
var confStateKey = []byte{1, 3}
var snapshotKey = []byte{1, 4}
var appliedKey = []byte{1, 5}
var clusterKey = []byte{1, 6}
var nodeKey = []byte{1, 7}
var snapshotMagic = []byte("CHRSNP01")
var jsonSnappyMagic = []byte{0, 'C', 'H', 'R', 'J', 1}

const maxJSONRecordBytes = 64 << 20
const minJSONCompressionBytes = 256

func numberKey(prefix byte, value uint64) []byte {
	key := make([]byte, 9)
	key[0] = prefix
	// Big-endian indices preserve numeric ordering under bytewise scans
	binary.BigEndian.PutUint64(key[1:], value)
	return key
}

func segmentedKey(prefix byte, values ...string) []byte {
	size := 1
	for _, value := range values {
		size += 4 + len(value)
	}
	key := make([]byte, 1, size)
	key[0] = prefix
	for _, value := range values {
		var length [4]byte
		binary.BigEndian.PutUint32(length[:], uint32(len(value)))
		key = append(key, length[:]...)
		key = append(key, value...)
	}
	return key
}

func prefixBounds(prefix byte) ([]byte, []byte) {
	return []byte{prefix}, []byte{prefix + 1}
}

func prefixUpper(prefix []byte) []byte {
	upper := append([]byte(nil), prefix...)
	for index := len(upper) - 1; index >= 0; index-- {
		if upper[index] != 0xff {
			upper[index]++
			return upper[:index+1]
		}
	}
	return nil
}

func encodeJSON(value any) ([]byte, error) {
	data, err := json.Marshal(value)
	if err != nil {
		return data, err
	}
	if len(data) < minJSONCompressionBytes || len(data) > maxJSONRecordBytes {
		return data, nil
	}
	encoded := make([]byte, len(jsonSnappyMagic)+snappy.MaxEncodedLen(len(data)))
	compressed := snappy.Encode(encoded[len(jsonSnappyMagic):], data)
	if len(jsonSnappyMagic)+len(compressed) >= len(data) {
		return data, nil
	}
	copy(encoded, jsonSnappyMagic)
	return encoded[:len(jsonSnappyMagic)+len(compressed)], nil
}

func decodeJSON(data []byte, value any) error {
	if bytes.HasPrefix(data, jsonSnappyMagic) {
		decodedLength, err := snappy.DecodedLen(data[len(jsonSnappyMagic):])
		if err != nil {
			return err
		}
		if decodedLength > maxJSONRecordBytes {
			return fmt.Errorf("json record exceeds size limit")
		}
		decoded, err := snappy.Decode(nil, data[len(jsonSnappyMagic):])
		if err != nil {
			return err
		}
		data = decoded
	}
	if err := json.Unmarshal(data, value); err != nil {
		return err
	}
	return nil
}

func encodeProto(value proto.Message) ([]byte, error) {
	return proto.MarshalOptions{Deterministic: true}.Marshal(value)
}

func encodeSnapshotImage(image SnapshotImage) ([]byte, error) {
	payload, err := json.Marshal(image)
	if err != nil {
		return nil, err
	}
	data := make([]byte, 16+len(payload)+sha256.Size)
	copy(data, snapshotMagic)
	binary.BigEndian.PutUint64(data[8:16], uint64(len(payload)))
	copy(data[16:], payload)
	// Digest framing and payload together
	hash := sha256.Sum256(data[:16+len(payload)])
	copy(data[16+len(payload):], hash[:])
	return data, nil
}

func decodeSnapshotImage(data []byte) (SnapshotImage, error) {
	if len(data) < 16+sha256.Size || !bytes.Equal(data[:8], snapshotMagic) {
		return SnapshotImage{}, ErrCorruptSnapshot
	}
	length := binary.BigEndian.Uint64(data[8:16])
	if length > 4<<30 || length != uint64(len(data)-16-sha256.Size) {
		return SnapshotImage{}, ErrCorruptSnapshot
	}
	payloadEnd := 16 + int(length)
	hash := sha256.Sum256(data[:payloadEnd])
	if !bytes.Equal(hash[:], data[payloadEnd:]) {
		return SnapshotImage{}, ErrCorruptSnapshot
	}
	var image SnapshotImage
	if err := json.Unmarshal(data[16:payloadEnd], &image); err != nil {
		return SnapshotImage{}, fmt.Errorf("%w: %v", ErrCorruptSnapshot, err)
	}
	if image.Version != SnapshotVersion {
		return SnapshotImage{}, ErrRecordVersion
	}
	return image, nil
}
