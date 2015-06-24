package storage

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"path"
	"testing"
	"time"

	"github.com/lilwulin/rabbitfs/helper"
	"github.com/visionmedia/go-bench"
)

var (
	inputPath         string
	outputPath        string
	outputDeletedPath string
	pic1Name          string
	pic2Name          string
)

func init() {
	inputPath = "./testData/input"
	outputPath = "./testData/output"
	outputDeletedPath = "./testData/output-after-deleted"
	pic1Name = "Massimo.jpg"
	pic2Name = "panda.jpg"
}

func printTestInfo(head string) {
	fmt.Println(head)
	fmt.Println("======================")
}

func TestBehavior(t *testing.T) {
	printTestInfo("TESTING BEHAVIOR")
	defer helper.RemoveDirs("./testData/data", "./test_mapping")

	testKey1 := 0
	testCookie1 := rand.Uint32()

	testKey2 := 1
	testCookie2 := rand.Uint32()

	var vol *Volume
	var err error
	fmt.Println("Open or Create data file")
	file, err := os.OpenFile("./testData/data", os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		t.Error(err)
	}
	fmt.Println("Create Volume")
	vol, err = NewVolume(0, file, "./test_mapping", 0.4)
	if err != nil {
		t.Error(err)
	}

	fmt.Println("Get non-existent key-cookie")
	if _, _, err = vol.mapping.Get(1024, 1024); err == nil {
		t.Error("expect error not found")
	}

	// Input
	var n1I, n2I *Needle
	var f1DataI, f2DataI []byte
	// Output
	var n1O, n2O *Needle
	var f1DataO, f2DataO []byte

	fmt.Println("Read image 1")
	if f1DataI, err = ioutil.ReadFile(path.Join(inputPath, pic1Name)); err != nil {
		t.Error(err)
	}
	fmt.Println("Read image 2")
	if f2DataI, err = ioutil.ReadFile(path.Join(inputPath, pic2Name)); err != nil {
		t.Error(err)
	}
	fmt.Println("Create Needle 1")
	n1I = NewNeedle(testCookie1, uint64(testKey1), f1DataI, []byte(pic1Name))
	fmt.Println("Create Needle 2")
	n2I = NewNeedle(testCookie2, uint64(testKey2), f2DataI, []byte(pic2Name))
	fmt.Println("Append Needle 1")
	if err = vol.AppendNeedle(n1I); err != nil {
		t.Error(err)
	}
	fmt.Println("Append Needle 2")
	if err = vol.AppendNeedle(n2I); err != nil {
		t.Error(err)
	}
	fmt.Println("Get Needle 1")
	if n1O, err = vol.GetNeedle(uint64(testKey1), testCookie1); err != nil {
		t.Error(err)
	}
	fmt.Println("Get Needle 2")
	if n2O, err = vol.GetNeedle(uint64(testKey2), testCookie2); err != nil {
		t.Error(err)
	}

	f1DataO = n1O.Data
	f2DataO = n2O.Data
	fmt.Println("Data 1 input and output shoud be the same")
	if bytes.Compare(f1DataI, f1DataO) != 0 {
		t.Error("input and output data should be the same")
	}
	fmt.Println("Data 2 input and output shoud be the same")
	if bytes.Compare(f2DataI, f2DataO) != 0 {
		t.Error("input and output data should be the same")
	}
	fmt.Println("Write images to output dir")
	if err = os.MkdirAll(outputPath, 0777); err != nil {
		t.Error(err)
	}
	if err = ioutil.WriteFile(path.Join(outputPath, string(n1O.Name)), f1DataO, 0777); err != nil {
		t.Error(err)
	}
	if err = ioutil.WriteFile(path.Join(outputPath, string(n2O.Name)), f2DataO, 0777); err != nil {
		t.Error(err)
	}

	fmt.Println("Delete Needle 1")
	oldFileInfo, _ := vol.StoreFile.Stat()
	fmt.Println("the old StoreFile size is ", oldFileInfo.Size())
	if err := vol.DelNeedle(uint64(testKey1), testCookie1); err != nil {
		t.Error(err)
	}
	_, err = vol.GetNeedle(uint64(testKey1), testCookie1)
	if err == nil {
		t.Error("expect error, but got nil")
	} else {
		fmt.Println(err)
	}
	time.Sleep(200 * time.Millisecond)
	newFileInfo, _ := vol.StoreFile.Stat()
	fmt.Println("the new StoreFile size is ", newFileInfo.Size())
	if oldFileInfo.Size() <= newFileInfo.Size() {
		t.Errorf("expect old StoreFile size to be bigger than new StoreFile size, but got old size:%d, new size:%d\n",
			oldFileInfo.Size(), newFileInfo.Size())
	}
	fmt.Println("Get Needle 2")
	if n2O, err = vol.GetNeedle(uint64(testKey2), testCookie2); err != nil {
		t.Error(err)
	}
	fmt.Println("Write images to output-after-deleted")
	f2DataO = n2O.Data
	if err = ioutil.WriteFile(path.Join(outputDeletedPath, string(n2O.Name)), f2DataO, 0777); err != nil {
		t.Error(err)
	}
}

func TestNameTooLong(t *testing.T) {
	printTestInfo("TESTING NAME TOO LONG")
	cookie := 1
	key := 1
	data := []byte(string("hey"))
	var name []byte
	for i := 1; i <= 256; i++ {
		name = append(name, 1)
	}
	fmt.Println("Get New Needle")
	n := NewNeedle(uint32(cookie), uint64(key), data, name)
	if n.NameSize > 0 {
		t.Errorf("expect NameSize to be 0 but got %d", n.NameSize)
	}
	if len(n.Name) > 0 {
		t.Errorf("expect name to be empty but got %s", string(n.Name))
	}
}

func BenchmarkWriteAndRead(b *testing.B) {
	printTestInfo("BENCHMARKING")
	vol, f1DataI := getVolAndData()
	ops := 5000
	ben := bench.Start("Append-Needles-5000")
	for i := 0; i < ops; i++ {
		vol.AppendNeedle(NewNeedle(uint32(i), uint64(i), f1DataI, []byte(pic1Name)))
	}
	ben.End(ops)
	helper.RemoveDirs("./testData/data", "./test_mapping")

	vol, f1DataI = getVolAndData()
	ops = 10000
	ben = bench.Start("Append-Needles-10000")
	for i := 0; i < ops; i++ {
		vol.AppendNeedle(NewNeedle(uint32(i), uint64(i), f1DataI, []byte(pic1Name)))
	}
	ben.End(ops)
	helper.RemoveDirs("./testData/data", "./test_mapping")

	vol, f1DataI = getVolAndData()
	ops = 20000
	ben = bench.Start("Append-Needles-20000")
	for i := 0; i < ops; i++ {
		vol.AppendNeedle(NewNeedle(uint32(i), uint64(i), f1DataI, []byte(pic1Name)))
	}
	ben.End(ops)

	ops = 5000
	ben = bench.Start("Get-Needles-5000")
	for i := 0; i < ops; i++ {
		_, _ = vol.GetNeedle(uint64(i), uint32(i))
	}
	ben.End(ops)

	ops = 10000
	ben = bench.Start("Get-Needles-10000")
	for i := 0; i < ops; i++ {
		_, _ = vol.GetNeedle(uint64(i), uint32(i))
	}
	ben.End(ops)

	ops = 20000
	ben = bench.Start("Get-Needles-20000")
	for i := 0; i < ops; i++ {
		_, _ = vol.GetNeedle(uint64(i), uint32(i))
	}
	ben.End(ops)
	helper.RemoveDirs("./testData/data", "./test_mapping")
}

func getVolAndData() (*Volume, []byte) {
	file, _ := os.OpenFile("./testData/data", os.O_RDWR|os.O_CREATE, 0644)
	vol, _ := NewVolume(0, file, "./test_mapping", 0.4)
	f1DataI, _ := ioutil.ReadFile(path.Join(inputPath, pic1Name))
	return vol, f1DataI
}
