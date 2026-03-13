package protocol

type Encoder interface {
	TotalSize() int
	Encode(w *Writer, correlationID int32)
}

type Storable interface {
	TotalSize() int
	Encode(w *Writer)
}
