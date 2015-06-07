package storage

type Cleaner struct {
	VolumeIdChan chan uint32
}

func (c *Cleaner) On() {
	for {
		select {
		case <-c.VolumeIdChan:
			//???what to do???
		}
	}
}
