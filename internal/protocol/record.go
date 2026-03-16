package protocol

import (
	"encoding/binary"
	"fmt"
)

type Record struct {
	Offset int64
	Value  []byte
}

// DecodeRecordBatch parses a Kafka Record Batch (v2) and returns individual records.
// It can handle multiple batches in the same byte array.
func DecodeRecordBatch(data []byte) ([]Record, error) {
	var records []Record
	pos := 0

	for pos < len(data) {
		if len(data)-pos < 61 { // Minimum batch header size
			break
		}

		baseOffset := int64(binary.BigEndian.Uint64(data[pos : pos+8]))
		pos += 8

		batchLength := int32(binary.BigEndian.Uint32(data[pos : pos+4]))
		pos += 4

		// The rest of the batch is batchLength bytes long.
		if len(data)-pos < int(batchLength) {
			break
		}

		batchEnd := pos + int(batchLength)

		// Skip PartitionLeaderEpoch(4), Magic(1), CRC(4), Attributes(2), LastOffsetDelta(4), 
		// FirstTimestamp(8), MaxTimestamp(8), ProducerId(8), ProducerEpoch(2), BaseSequence(4)
		pos += 4 + 1 + 4 + 2 + 4 + 8 + 8 + 8 + 2 + 4

		recordsCount := int32(binary.BigEndian.Uint32(data[pos : pos+4]))
		pos += 4

		for i := 0; i < int(recordsCount); i++ {
			if pos >= batchEnd {
				break
			}

			// Record Length (Varint)
			recLen, n := binary.Varint(data[pos:])
			if n <= 0 {
				break
			}
			pos += n
			
			recordStart := pos
			recordEnd := recordStart + int(recLen)

			// Attributes (1 byte)
			pos += 1

			// TimestampDelta (Varint)
			_, n = binary.Varint(data[pos:])
			pos += n

			// OffsetDelta (Varint)
			offsetDelta, n := binary.Varint(data[pos:])
			pos += n

			// KeyLength (Varint)
			keyLen, n := binary.Varint(data[pos:])
			pos += n
			if keyLen > 0 {
				pos += int(keyLen)
			}

			// ValueLength (Varint)
			valLen, n := binary.Varint(data[pos:])
			pos += n
			
			var value []byte
			if valLen > 0 {
				value = make([]byte, valLen)
				copy(value, data[pos:pos+int(valLen)])
				pos += int(valLen)
			}

			records = append(records, Record{
				Offset: baseOffset + offsetDelta,
				Value:  value,
			})

			// HeadersCount (Varint) - skip headers
			pos = recordEnd
		}
		
		pos = batchEnd
	}

	return records, nil
}

func (r Record) String() string {
	return fmt.Sprintf("Offset: %d | Message: %s", r.Offset, string(r.Value))
}
