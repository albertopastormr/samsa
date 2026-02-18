package protocol

import "encoding/binary"

const (
	SizeInt8      = 1
	SizeInt16     = 2
	SizeInt32     = 4
	SizeTagBuffer = 1 // Compact types use 1 byte for empty tag buffer
)

type Reader struct {
	buf []byte
	pos int
}

func NewReader(buf []byte) *Reader {
	return &Reader{buf: buf, pos: 0}
}

func (r *Reader) ReadInt16() int16 {
	v := int16(binary.BigEndian.Uint16(r.buf[r.pos : r.pos+2]))
	r.pos += 2
	return v
}

func (r *Reader) ReadInt32() int32 {
	v := int32(binary.BigEndian.Uint32(r.buf[r.pos : r.pos+4]))
	r.pos += 4
	return v
}

func (r *Reader) Remaining() int {
	return len(r.buf) - r.pos
}

// Writer provides primitives to write Kafka wire protocol types without reflection.
type Writer struct {
	buf []byte
	pos int
}

func NewWriter(size int) *Writer {
	return &Writer{buf: make([]byte, size), pos: 0}
}

func (w *Writer) Bytes() []byte {
	return w.buf[:w.pos]
}

func (w *Writer) WriteInt16(v int16) {
	binary.BigEndian.PutUint16(w.buf[w.pos:w.pos+2], uint16(v))
	w.pos += 2
}

func (w *Writer) WriteInt32(v int32) {
	binary.BigEndian.PutUint32(w.buf[w.pos:w.pos+4], uint32(v))
	w.pos += 4
}

func (w *Writer) WriteUint8(b uint8) {
	w.buf[w.pos] = b
	w.pos++
}

func (w *Writer) WriteUint32(v uint32) {
	binary.BigEndian.PutUint32(w.buf[w.pos:w.pos+4], v)
	w.pos += 4
}
