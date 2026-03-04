package protocol

type Encoder interface {
	TotalSize() int
	Encode(w *Writer, correlationID int32)
}
