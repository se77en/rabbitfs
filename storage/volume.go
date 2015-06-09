package storage

import (
	"errors"
	"fmt"
	"os"
	"sync"

	"github.com/lilwulin/rabbitfs/log"
	"github.com/syndtr/goleveldb/leveldb"
)

// TODO: when to change readOnly???

const KeyDeletedSize = "key.deleted.size"

// Volume is formed by multiple Needles
type Volume struct {
	ID               uint32
	StoreFile        *os.File
	mapping          *Mapping
	mappingName      string
	fileLock         sync.RWMutex
	garbageThreshold float32
	readOnly         bool
	volTmp           *Volume
	isCleaning       bool
}

func NewVolume(id uint32, storeFile *os.File, mapFilePath string, threshold float32) (*Volume, error) {
	m, err := NewLevelDBMapping(mapFilePath) // TODO: this needs to be changed
	if err != nil {
		return nil, err
	}
	v := &Volume{
		ID:               id,
		StoreFile:        storeFile,
		mappingName:      mapFilePath,
		mapping:          m,
		garbageThreshold: threshold,
		readOnly:         false,
		isCleaning:       false,
	}
	// cleaner.newVolumeChan <- v
	return v, nil
}

// AppendNeedle appends needle to vol's StoreFile
func (vol *Volume) AppendNeedle(n *Needle) error {
	if vol.readOnly {
		return fmt.Errorf("volume %d is read-only", vol.ID)
	}
	vol.fileLock.Lock()
	defer vol.fileLock.Unlock()
	// cleaning process is very time-consuming.
	// so I think it's necessary to handle AppendNeedle
	// during cleaning
	if vol.isCleaning {
		return vol.volTmp.AppendNeedle(n)
	}
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
	if vol.isCleaning {
		// I think it's also necessary to handle GetNeedle
		// during cleaning
		return vol.volTmp.GetNeedle(key, cookie)
	}
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
// the Cleaner will reclaim the space occupied by deleted needle
func (vol *Volume) DelNeedle(key uint64, cookie uint32) error {
	_, size, _ := vol.mapping.Get(key, cookie)
	if size == 0 {
		return nil
	}
	vol.fileLock.Lock()
	defer vol.fileLock.Unlock()
	deletedSize, err := vol.increaseDeletedSize(uint64(size))
	if err != nil {
		return err
	}
	fi, err := vol.StoreFile.Stat()
	if err == nil {
		return err
	}
	if float32(deletedSize)/float32(fi.Size()) > vol.garbageThreshold {
		go func() {
			if err := vol.cleanNeedles(); err != nil {
				log.Errorln(err)
			}
		}()
	}
	return vol.mapping.Del(key, cookie)
}

func (vol *Volume) increaseDeletedSize(size uint64) (deletedSize uint64, err error) {
	key := []byte(KeyDeletedSize)
	sizeBytes, err := vol.mapping.kvstore.Get(key)
	if err != nil {
		if err == leveldb.ErrNotFound {
			val := make([]byte, 8)
			UInt64ToBytes(val, size)
			err = vol.mapping.kvstore.Put(key, val)
			return size, err
		}
		return
	}
	deletedSize = BytesToUInt64(sizeBytes)
	deletedSize += size
	deletedSizeByte := make([]byte, 8)
	UInt64ToBytes(deletedSizeByte, deletedSize)
	if err = vol.mapping.kvstore.Put(key, deletedSizeByte); err != nil {
		return 0, err
	}
	return
}

func (vol *Volume) cleanNeedles() error {
	log.Infoln(0, "volume%d is cleaning", vol.ID)
	newStoreFileName := vol.StoreFile.Name() + ".tmp"
	newStoreFile, err := os.OpenFile(newStoreFileName, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		return err
	}
	newMappingName := vol.mappingName + "_tmp"
	newMapping, err := NewLevelDBMapping(newMappingName) // TODO: this needs to be changed
	if err != nil {
		return err
	}
	vol.volTmp = &Volume{
		StoreFile: newStoreFile,
		mapping:   newMapping,
	}
	vol.isCleaning = true
	err = vol.mapping.Iter(func(key uint64, cookie uint32) error {
		n, err := vol.GetNeedle(key, cookie)
		if err != nil {
			return err
		}
		return vol.volTmp.AppendNeedle(n)
	})
	if err != nil {
		return err
	}
	oldFileInfo, err := vol.StoreFile.Stat() // save old StoreFile Info
	if err != nil {
		return err
	}

	vol.fileLock.Lock()
	defer func() {
		vol.isCleaning = false
		vol.volTmp = nil
		vol.fileLock.Unlock()
	}()
	// Switch StoreFile
	if err = os.RemoveAll(vol.StoreFile.Name()); err != nil { // remove the old StoreFIle
		return err
	}
	if err = newStoreFile.Sync(); err != nil {
		return err
	}
	if err = newStoreFile.Close(); err != nil { // save the new StoreFile to disk
		return err
	}
	// remove the ".tmp" in newStoreFile
	if err = os.Rename(newStoreFileName, newStoreFileName[:len(newStoreFileName)-4]); err != nil {
		return err
	}
	vol.StoreFile, err = os.OpenFile(newStoreFileName[:len(newStoreFileName)-4], os.O_RDWR, oldFileInfo.Mode())
	if err != nil {
		return err
	}
	// Switch Mapping
	if err = os.RemoveAll(vol.mappingName); err != nil {
		return err
	}
	if err = os.Rename(newMappingName, vol.mappingName); err != nil {
		return err
	}
	vol.mapping, err = NewLevelDBMapping(vol.mappingName)
	return err
}
