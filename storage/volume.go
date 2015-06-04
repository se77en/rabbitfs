package storage

import (
	"errors"
	"fmt"
	"os"
	"sync"
)

// Volume is formed by multiple Needles
type Volume struct {
	ID        uint32
	StoreFile *os.File
	mapping   *Mapping
	fileLock  sync.RWMutex
	readOnly  bool
}

func NewVolume(id uint32, storeFile *os.File, mapFilename string) (*Volume, error) {
	m, err := NewLevelDBMapping(mapFilename)
	if err != nil {
		return nil, err
	}
	return &Volume{
		ID:        id,
		StoreFile: storeFile,
		mapping:   m,
		readOnly:  true,
	}, nil
}

// AppendNeedle appends needle to vol's StoreFile
func (vol *Volume) AppendNeedle(n *Needle) error {
	if vol.readOnly {
		return fmt.Errorf("volume %d is read-only", vol.ID)
	}
	vol.fileLock.Lock()
	defer vol.fileLock.Unlock()
	offset, err := vol.StoreFile.Seek(0, os.SEEK_CUR)
	if err != nil {
		return err
	}
	if offset%NeedlePaddingSize != 0 {
		offset += NeedlePaddingSize - (offset % NeedlePaddingSize)
		if offset, err = vol.StoreFile.Seek(offset, os.SEEK_SET); err != nil {
			return err
		}
	}
	header := make([]byte, NeedleHeaderSize)
	UInt32ToBytes(header[0:4], n.Cookie)
	UInt64ToBytes(header[4:12], n.Key)
	UInt32ToBytes(header[12:16], n.Size)
	if _, err = vol.StoreFile.Write(header); err != nil {
		return err
	}
	if _, err = vol.StoreFile.Write(n.Data); err != nil {
		return err
	}
	csbytes := make([]byte, 4)
	UInt32ToBytes(csbytes, n.CheckSum)
	if _, err = vol.StoreFile.Write(csbytes); err != nil {
		return err
	}
	// n.Padding = make([]byte, NeedlePaddingSize-(NeedleHeaderSize+n.Size+NeedleChecksumSize)%NeedlePaddingSize)
	if _, err = vol.StoreFile.Write(n.Padding); err != nil {
		return err
	}
	// Add this <key,cookie>-<offset,size> pair to mapping
	return vol.mapping.Put(n.Key, n.Cookie, uint32(offset), n.Size)
}

// GetNeedle gets the needle from volume by given <key, cookie>
func (vol *Volume) GetNeedle(key uint64, cookie uint32) (*Needle, error) {
	offset, size, err := vol.mapping.Get(key, cookie)
	if err != nil {
		return nil, err
	}
	vol.fileLock.RLock()
	defer vol.fileLock.RUnlock()
	wholeSize := NeedleHeaderSize + size + NeedleChecksumSize
	needleBytes := make([]byte, wholeSize)
	readSize, err := vol.StoreFile.ReadAt(needleBytes, int64(offset))
	if err != nil {
		return nil, err
	}
	if uint32(readSize) != wholeSize {
		return nil, fmt.Errorf("expected size %d, get size %d", wholeSize, readSize)
	}
	data := needleBytes[NeedleHeaderSize:size]
	checkSum := BytesToUInt32(needleBytes[NeedleHeaderSize+size : wholeSize])
	nCheckSum := newCheckSum(data)
	if checkSum != nCheckSum {
		return nil, errors.New("data on disk corrupted")
	}
	return &Needle{
		Cookie:   BytesToUInt32(needleBytes[0:4]),
		Key:      BytesToUInt64(needleBytes[4:12]),
		Size:     BytesToUInt32(needleBytes[12:NeedleHeaderSize]),
		Data:     data,
		CheckSum: checkSum,
	}, nil
}

func (vol *Volume) DelNeedle(key uint64, cookie uint32) error {
	_, _, err := vol.mapping.Get(key, cookie)
	if err != nil {
		return err
	}
	return vol.mapping.Del(key, cookie)
}
