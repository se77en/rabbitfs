package storage

var cleaner *Cleaner

func init() {
	cleaner = &Cleaner{
		volumeIDMap:   make(map[uint32]*Volume),
		cleanIDChan:   make(chan uint32, 10),
		newVolumeChan: make(chan *Volume, 10),
	}
	go func() { cleaner.On() }()
}

// Cleaner run through the whole volume's StoreFile and copy
// all but deleted needles to another StoreFile. The volumes
// will activate Cleaner when they reach their garbageThreshold.
type Cleaner struct {
	volumeIDMap   map[uint32]*Volume
	cleanIDChan   chan uint32
	newVolumeChan chan *Volume
}

// Turn on the Cleaner
func (c *Cleaner) On() {
	// TODO: Add more
	for {
		select {
		case vol := <-c.newVolumeChan:
			c.volumeIDMap[vol.ID] = vol
			// case id := <-c.cleanIDChan:
		}
	}
}
