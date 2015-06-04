package storage

const (
	NeedleHeaderSize   = 16 //NeedleHeaderSize = sizeof(Cookie)+sizeof(Key)+sizeof(Size)
	NeedlePaddingSize  = 8  // Total needle size is aligned to 8 bytes
	NeedleChecksumSize = 4
)

// Needle is the unit stored in volume.
// It contains header(Cookie, Key, Data Size),
// file data, checksum, and padding.
type Needle struct {
	Cookie   uint32
	Key      uint64
	Size     uint32 // the size of Data
	Data     []byte
	CheckSum uint32
	Padding  []byte
	// TODO: Add more data
}

func NewNeedle(cookie uint32, key uint64, data []byte) *Needle {
	size := uint32(len(data))
	return &Needle{
		Cookie:   cookie,
		Key:      key,
		Size:     size,
		Data:     data,
		CheckSum: newCheckSum(data),
		Padding:  make([]byte, NeedlePaddingSize-(NeedleHeaderSize+size+NeedleChecksumSize)%NeedlePaddingSize),
	}
}
