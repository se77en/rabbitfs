package commandline

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"runtime"
	"time"

	"github.com/lilwulin/rabbitfs/server"
)

var StoreCmd = &Command{}

func init() {
	StoreCmd.Name = "store"
	StoreCmd.Run = cmdStoreRun
}

var (
	storePort        = StoreCmd.Flag.Int("port", 8666, "http listen port")
	storeIP          = StoreCmd.Flag.String("ip", "127.0.0.1", "ip address")
	storeConfPath    = StoreCmd.Flag.String("confpath", filepath.Join(os.TempDir(), "rabbitfs"), "configuration path")
	volumeDir        = StoreCmd.Flag.String("volumedir", filepath.Join(os.TempDir(), "rabbitfs"), "the path to store volume file")
	garbageThreshold = StoreCmd.Flag.Float64("garbage_threshold", 0.4, "volume will start cleaning deleted files when reaching the threshold")
	storeTimeout     = StoreCmd.Flag.Int64("timeout", 10000, "maximum duration(in millisecond) before server timing out")
)

func cmdStoreRun(args []string) error {
	if *cpu < 1 {
		*cpu = runtime.NumCPU()
	}
	runtime.GOMAXPROCS(*cpu)
	// if _, err := os.Stat(*storeConfPath); err != nil {
	// 	return err
	// }
	// confFilePath := filepath.Join(*storeConfPath, "rabbitfs.conf.json")
	// if _, err := os.Stat(confFilePath); err != nil {
	// 	if os.IsNotExist(err) {
	// 		if err = ioutil.WriteFile(confFilePath, []byte(defaultConfig), 0644); err != nil {
	// 			return err
	// 		}
	// 	}
	// 	return err
	// }
	if _, err := os.Stat(*storeConfPath); err != nil {
		if os.IsNotExist(err) {
			if err = os.MkdirAll(*storeConfPath, os.ModePerm); err != nil {
				return err
			}
			if err = ioutil.WriteFile(filepath.Join(*storeConfPath, "rabbitfs.conf.json"), []byte(defaultConfig), os.ModePerm); err != nil {
				return err
			}
		} else {
			return err
		}
	}
	httpAddr := fmt.Sprintf("%s:%d", *storeIP, *storePort)
	store, err := server.NewStoreServer(
		*storeConfPath,
		*volumeDir,
		float32(*garbageThreshold),
		httpAddr,
		time.Duration((*storeTimeout))*time.Millisecond,
	)
	if err != nil {
		return err
	}
	store.ListenAndServe()
	return nil
}
