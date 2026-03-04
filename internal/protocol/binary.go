package protocol

import "encoding/binary"

const (
	SizeInt8      = 1
	SizeInt16     = 2
	SizeInt32     = 4
	SizeTagBuffer = 1 // Compact types use 1 byte for empty tag buffer
)

type Reader struct {
	Buf []byte
	Pos int
}

func (r *Reader) ReadVarint() (uint64, int) {
	v, n := binary.Uvarint(r.Buf[r.Pos:])
	r.Pos += n
	return v, n
}

func NewReader(buf []byte) *Reader {
	return &Reader{Buf: buf, Pos: 0}
}

func (r *Reader) ReadInt16() int16 {
	v := int16(binary.BigEndian.Uint16(r.Buf[r.Pos : r.Pos+2]))
	r.Pos += 2
	return v
}

func (r *Reader) ReadInt32() int32 {
	v := int32(binary.BigEndian.Uint32(r.Buf[r.Pos : r.Pos+4]))
	r.Pos += 4
	return v
}

func (r *Reader) ReadInt64() int64 {
	v := int64(binary.BigEndian.Uint64(r.Buf[r.Pos : r.Pos+8]))
	r.Pos += 8
	return v
}

func (r *Reader) ReadUint8() uint8 {
	v := r.Buf[r.Pos]
	r.Pos += 1
	return v
}

func (r *Reader) ReadBytes(b []byte) {
	copy(b, r.Buf[r.Pos:r.Pos+len(b)])
	r.Pos += len(b)
}

func (r *Reader) Remaining() int {
	return len(r.Buf) - r.Pos
}

// Writer provides primitives to write Kafka wire protocol types without reflection.
type Writer struct {
	Buf []byte
	Pos int
}

func NewWriter(size int) *Writer {
	return &Writer{Buf: make([]byte, size), Pos: 0}
}

func (w *Writer) Bytes() []byte {
	return w.Buf[:w.Pos]
}

func (w *Writer) WriteInt16(v int16) {
	binary.BigEndian.PutUint16(w.Buf[w.Pos:w.Pos+2], uint16(v))
	w.Pos += 2
}

func (w *Writer) WriteInt32(v int32) {
	binary.BigEndian.PutUint32(w.Buf[w.Pos:w.Pos+4], uint32(v))
	w.Pos += 4
}

func (w *Writer) WriteVarint(v uint64) {
	n := binary.PutUvarint(w.Buf[w.Pos:], v)
	w.Pos += n
}

func (w *Writer) WriteUint8(b uint8) {
	w.Buf[w.Pos] = b
	w.Pos++
}

func (w *Writer) WriteUint32(v uint32) {
	binary.BigEndian.PutUint32(w.Buf[w.Pos:w.Pos+4], v)
	w.Pos += 4
}

func (w *Writer) WriteInt64(v int64) {
	binary.BigEndian.PutUint64(w.Buf[w.Pos:w.Pos+8], uint64(v))
	w.Pos += 8
}

func (r *Reader) ReadInt8() int8 {
	v := int8(r.Buf[r.Pos])
	r.Pos += 1
	return v
}

func (r *Reader) ReadNullableString() *string {
	length := r.ReadInt16()
	if length == -1 {
		return nil
	}
	v := string(r.Buf[r.Pos : r.Pos+int(length)])
	r.Pos += int(length)
	return &v
}

func (r *Reader) ReadCompactString() string {
	lengthVar, _ := r.ReadVarint()
	length := int(lengthVar) - 1
	if length <= 0 {
		return ""
	}
	v := string(r.Buf[r.Pos : r.Pos+length])
	r.Pos += length
	return v
}

func (w *Writer) WriteInt8(v int8) {
	w.Buf[w.Pos] = byte(v)
	w.Pos++
}

func (w *Writer) WriteCompactString(s string) {
	w.WriteVarint(uint64(len(s) + 1))
	copy(w.Buf[w.Pos:], s)
	w.Pos += len(s)
}

func (w *Writer) WriteBytes(b []byte) {
	copy(w.Buf[w.Pos:], b)
	w.Pos += len(b)
}
