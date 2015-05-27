package storage

// Volume maps to many physicVolumes for redundancy,
// actual file storage happens in physicVolume
type Volume struct {
	ID  uint32
	pvs []physicVolume
}

type physicVolume struct {
}
