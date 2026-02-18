package protocol

import (
	"bytes"
	"testing"
)

func TestReader(t *testing.T) {
	buf := []byte{0x00, 0x01, 0x00, 0x00, 0x00, 0x02}
	r := NewReader(buf)

	if v := r.ReadInt16(); v != 1 {
		t.Errorf("ReadInt16() = %d; want 1", v)
	}

	if v := r.ReadInt32(); v != 2 {
		t.Errorf("ReadInt32() = %d; want 2", v)
	}

	if r.Remaining() != 0 {
		t.Errorf("Remaining() = %d; want 0", r.Remaining())
	}
}

func TestWriter(t *testing.T) {
	w := NewWriter(10)
	w.WriteInt16(1)
	w.WriteInt32(2)
	w.WriteByte(0xFF)
	w.WriteUint32(0x12345678)

	expected := []byte{
		0x00, 0x01, // Int16(1)
		0x00, 0x00, 0x00, 0x02, // Int32(2)
		0xFF,                   // Byte(0xFF)
		0x12, 0x34, 0x56, 0x78, // Uint32(...)
	}

	if !bytes.Equal(w.Bytes(), expected) {
		t.Errorf("Writer.Bytes() = %x; want %x", w.Bytes(), expected)
	}
}
