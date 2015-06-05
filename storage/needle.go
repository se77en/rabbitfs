package storage

const (
	NeedleHeaderSize   = 16 // NeedleHeaderSize = sizeof(Cookie)+sizeof(Key)+sizeof(Size)
	NeedlePaddingSize  = 8  // Total needle size is aligned to 8 bytes
	NeedleChecksumSize = 4
	NeedleNameSize     = 256
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
	NameSize uint8
	Name     []byte
	Padding  []byte
	// TODO: Add more data
}

// NewNeedle returns a new needle for volume
func NewNeedle(cookie uint32, key uint64, data []byte, name []byte) *Needle {
	nameSize := len(name)
	if nameSize >= 256 {
		nameSize = 0
		name = make([]byte, 0)
	}
	size := len(data)
	needleSize := NeedleHeaderSize + size + NeedleChecksumSize + 1 + nameSize
	return &Needle{
		Cookie:   cookie,
		Key:      key,
		Size:     uint32(size),
		Data:     data,
		CheckSum: newCheckSum(data),
		NameSize: uint8(nameSize),
		Name:     name,
		Padding:  make([]byte, NeedlePaddingSize-(needleSize)%NeedlePaddingSize),
	}
}

func (n *Needle) fullSize() uint32 {
	return NeedleHeaderSize + n.Size + NeedleChecksumSize + 1 + uint32(n.NameSize)
}
