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

func NewVolume(id uint32, storeFile *os.File, mapFilePath string) (*Volume, error) {
	m, err := NewLevelDBMapping(mapFilePath)
	if err != nil {
		return nil, err
	}
	return &Volume{
		ID:        id,
		StoreFile: storeFile,
		mapping:   m,
		readOnly:  false,
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

	if _, err = vol.StoreFile.Write([]byte{n.NameSize}); err != nil {
		return err
	}

	if _, err = vol.StoreFile.Write(n.Name); err != nil {
		return err
	}

	if _, err = vol.StoreFile.Write(n.Padding); err != nil {
		return err
	}
	// Add this <key,cookie>-<offset,size> pair to mapping
	return vol.mapping.Put(n.Key, n.Cookie, uint32(offset), n.fullSize())
}

// GetNeedle gets the needle from volume by given <key, cookie>
func (vol *Volume) GetNeedle(key uint64, cookie uint32) (*Needle, error) {
	offset, fullsize, err := vol.mapping.Get(key, cookie)
	if err != nil {
		return nil, err
	}
	vol.fileLock.RLock()
	defer vol.fileLock.RUnlock()
	needleBytes := make([]byte, fullsize)
	readSize, err := vol.StoreFile.ReadAt(needleBytes, int64(offset))
	if err != nil {
		return nil, err
	}
	if uint32(readSize) != fullsize {
		return nil, fmt.Errorf("expected size %d, get size %d", fullsize, readSize)
	}
	ncookie := BytesToUInt32(needleBytes[0:4])
	nkey := BytesToUInt64(needleBytes[4:12])
	size := BytesToUInt32(needleBytes[12:NeedleHeaderSize])
	data := needleBytes[NeedleHeaderSize : NeedleHeaderSize+size]
	checkSum := BytesToUInt32(needleBytes[NeedleHeaderSize+size : NeedleHeaderSize+size+4])
	nCheckSum := newCheckSum(data)
	if checkSum != nCheckSum {
		return nil, errors.New("data on disk corrupted")
	}
	nameSizeIndex := NeedleHeaderSize + size + 4 // 4 means skip checksum
	nameSize := needleBytes[nameSizeIndex]
	nameIndex := nameSizeIndex + 1
	name := needleBytes[nameIndex : nameIndex+uint32(nameSize)]
	return &Needle{
		Cookie:   ncookie,
		Key:      nkey,
		Size:     size,
		Data:     data,
		CheckSum: checkSum,
		NameSize: nameSize,
		Name:     name,
	}, nil
}

// DelNeedle delete the <key,cookie>-<offset,size> pair in mapping
// another go routine will reclaim the space occupied by deleted needle
func (vol *Volume) DelNeedle(key uint64, cookie uint32) error {
	return vol.mapping.Del(key, cookie)
}
