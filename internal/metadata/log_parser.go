package metadata

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
)

type Topic struct {
	Name    string
	TopicId [16]byte
}

type Partition struct {
	PartitionId int32
	TopicId     [16]byte
	Replicas    []int32
	Isr         []int32
	Leader      int32
	LeaderEpoch int32
}

func parseCompactString(b []byte, offset *int) string {
	length, n := binary.Uvarint(b[*offset:])
	*offset += n
	l := int(length) - 1
	if l < 0 {
		return ""
	}
	s := string(b[*offset : *offset+l])
	*offset += l
	return s
}

func parseCompactInt32Array(b []byte, offset *int) []int32 {
	length, n := binary.Uvarint(b[*offset:])
	*offset += n
	l := int(length) - 1
	if l <= 0 {
		return nil
	}
	arr := make([]int32, l)
	for i := 0; i < l; i++ {
		arr[i] = int32(binary.BigEndian.Uint32(b[*offset : *offset+4]))
		*offset += 4
	}
	return arr
}

func parseTopicRecord(b []byte, version int) Topic {
	offset := 0
	name := parseCompactString(b, &offset)
	var topicId [16]byte
	copy(topicId[:], b[offset:offset+16])
	offset += 16

	// skip tags
	return Topic{Name: name, TopicId: topicId}
}

func parsePartitionRecord(b []byte, version int) Partition {
	offset := 0
	partitionId := int32(binary.BigEndian.Uint32(b[offset : offset+4]))
	offset += 4

	var topicId [16]byte
	copy(topicId[:], b[offset:offset+16])
	offset += 16

	replicas := parseCompactInt32Array(b, &offset)
	isr := parseCompactInt32Array(b, &offset)
	_ = parseCompactInt32Array(b, &offset) // removing
	_ = parseCompactInt32Array(b, &offset) // adding

	leader := int32(binary.BigEndian.Uint32(b[offset : offset+4]))
	offset += 4

	leaderEpoch := int32(binary.BigEndian.Uint32(b[offset : offset+4]))
	offset += 4

	// PartitionEpoch
	if offset+4 <= len(b) {
		_ = int32(binary.BigEndian.Uint32(b[offset : offset+4]))
		offset += 4
	}

	return Partition{
		PartitionId: partitionId,
		TopicId:     topicId,
		Replicas:    replicas,
		Isr:         isr,
		Leader:      leader,
		LeaderEpoch: leaderEpoch,
	}
}

// ReadClusterMetadata parses the log segments and returns maps of UUID->Topic and UUID->[]Partition
func ReadClusterMetadata(filepath string) (map[string]Topic, map[string][]Partition, error) {
	file, err := os.Open(filepath)
	if err != nil {
		return nil, nil, err
	}
	defer file.Close()

	topics := make(map[string]Topic)
	partitions := make(map[string][]Partition)

	for {
		var offset64 int64
		err := binary.Read(file, binary.BigEndian, &offset64)
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, nil, err
		}

		var batchLength int32
		if err := binary.Read(file, binary.BigEndian, &batchLength); err != nil {
			return nil, nil, err
		}

		batchData := make([]byte, batchLength)
		if _, err := io.ReadFull(file, batchData); err != nil {
			return nil, nil, err
		}

		// Parse the batch
		// BaseOffset(8) was already read outside batchLength, Wait: batchLength is size of REST of batch?
		// BaseOffset is 8 bytes. BatchLength is 4 bytes. Total batch size is 8 + 4 + BatchLength.
		// So batchData contains exactly from PartitionLeaderEpoch onwards.

		// Attributes is at offset 16-12 = 4 ?
		// Wait, PartitionLeaderEpoch (4) + Magic(1) + CRC(4) + Attributes(2) + LastOffsetDelta(4) ...
		// Let's just track position.
		pos := 0
		_ = int32(binary.BigEndian.Uint32(batchData[pos : pos+4])) // leader epoch
		pos += 4
		magic := batchData[pos]
		pos += 1
		_ = batchData[pos : pos+4] // crc
		pos += 4
		_ = int16(binary.BigEndian.Uint16(batchData[pos : pos+2]))
		pos += 2
		_ = batchData[pos : pos+4] // last offset delta
		pos += 4
		_ = batchData[pos : pos+8] // first ts
		pos += 8
		_ = batchData[pos : pos+8] // max ts
		pos += 8
		_ = batchData[pos : pos+8] // producer id
		pos += 8
		_ = batchData[pos : pos+2] // producer epoch
		pos += 2
		_ = batchData[pos : pos+4] // base seq
		pos += 4
		recordsCount := int32(binary.BigEndian.Uint32(batchData[pos : pos+4]))
		pos += 4

		if magic < 2 {
			continue // Log format prior to magic 2 is not used for KRaft usually
		}

		for i := 0; i < int(recordsCount); i++ {
			// Record Length
			recLen, n := binary.Varint(batchData[pos:])
			if n <= 0 {
				break
			}
			pos += n

			recStart := pos

			_ = batchData[pos] // attributes
			pos += 1

			_, n = binary.Varint(batchData[pos:]) // timestamp delta
			pos += n

			_, n = binary.Varint(batchData[pos:]) // offset delta
			pos += n

			keyLen, n := binary.Varint(batchData[pos:])
			pos += n
			if keyLen > 0 {
				pos += int(keyLen)
			}

			valLen, n := binary.Varint(batchData[pos:])
			pos += n

			if valLen > 0 {
				valBytes := batchData[pos : pos+int(valLen)]
				pos += int(valLen)

				// parse metadata record
				frameVersion := valBytes[0]
				offset := 1
				recordType, n1 := binary.Uvarint(valBytes[offset:])
				offset += n1
				recordVersion, n2 := binary.Uvarint(valBytes[offset:])
				offset += n2

				fmt.Printf("Parsed Record: FrameVersion=%d, Type=%d, Version=%d\n", frameVersion, recordType, recordVersion)

				if frameVersion == 0 {
					switch recordType {
					case 2: // TopicRecord
						fmt.Printf("Parsing TopicRecord\n")
						t := parseTopicRecord(valBytes[offset:], int(recordVersion))
						topics[string(t.TopicId[:])] = t
					case 3: // PartitionRecord
						fmt.Printf("Parsing PartitionRecord\n")
						p := parsePartitionRecord(valBytes[offset:], int(recordVersion))
						partitions[string(p.TopicId[:])] = append(partitions[string(p.TopicId[:])], p)
					case 12: // FeatureLevel
						fmt.Printf("Parsing FeatureLevel\n")
					}
				}
			} else {
				fmt.Printf("valLen was %d\n", valLen)
			}

			// skip headers? We have to fully jump by recLen anyway.
			// Re-anchor pos to start of record + recLen
			pos = recStart + int(recLen)
		}
	}

	return topics, partitions, nil
}
