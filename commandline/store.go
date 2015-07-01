package commandline

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"runtime"
	"time"

	"code.google.com/p/log4go"

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
	storeConfPath    = StoreCmd.Flag.String("confpath", "/etc/rabbitfs", "configuration path")
	volumeDir        = StoreCmd.Flag.String("volumedir", "/etc/rabbitfs", "the path to store volume file")
	garbageThreshold = StoreCmd.Flag.Float64("garbage_threshold", 0.4, "volume will start cleaning deleted files when reaching the threshold")
	storeTimeout     = StoreCmd.Flag.Int64("timeout", 10000, "maximum duration(in millisecond) before server timing out")
)

func cmdStoreRun(args []string) error {
	if *cpu < 1 {
		*cpu = runtime.NumCPU()
	}
	runtime.GOMAXPROCS(*cpu)
	log4go.Info("configuration path: %s", *storeConfPath)
	log4go.Info("volume path: %s", *volumeDir)
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
