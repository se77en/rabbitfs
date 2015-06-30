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

var DirCmd = &Command{}

func init() {
	DirCmd.Name = "directory"
	DirCmd.Run = cmdDirRun
}

var (
	dirPort       = DirCmd.Flag.Int("port", 9666, "http listen port")
	dirIP         = DirCmd.Flag.String("ip", "127.0.0.1", "ip address")
	dirConfPath   = DirCmd.Flag.String("confpath", filepath.Join(os.TempDir(), "rabbitfs"), "configuration path")
	maxVolumeSize = DirCmd.Flag.Int64("max_volume_size", 500, "max size of the volume file in MB")
	pulse         = DirCmd.Flag.Int64("pulse", 2000, "the interval(in millisecond) of directory server polling store server")
	timeout       = DirCmd.Flag.Int64("timeout", 10000, "maximum duration(in millisecond) before server timing out")
	raftPulse     = DirCmd.Flag.Int64("raft_pulse", 0, "the interval(in millisecond) of raft server polling store server")
	raftTimeout   = DirCmd.Flag.Int64("raft_timeout", 0, "maximum duration(in millisecond) before raft server timing out")
	cpu           = DirCmd.Flag.Int("cpu", 0, "maximum number of cpu")
)

func cmdDirRun(args []string) error {
	if *cpu < 1 {
		*cpu = runtime.NumCPU()
	}
	runtime.GOMAXPROCS(*cpu)

	if _, err := os.Stat(*dirConfPath); err != nil {
		if os.IsNotExist(err) {
			if err = os.MkdirAll(*dirConfPath, os.ModePerm); err != nil {
				return err
			}
			if err = ioutil.WriteFile(filepath.Join(*dirConfPath, "rabbitfs.conf.json"), []byte(defaultConfig), os.ModePerm); err != nil {
				return err
			}
		} else {
			return err
		}
	}

	httpAddr := fmt.Sprintf("%s:%d", *dirIP, *dirPort)
	dir, err := server.NewDirectory(
		*dirConfPath,
		httpAddr,
		time.Duration((*pulse))*time.Millisecond,
		*maxVolumeSize,
		time.Duration((*timeout))*time.Millisecond,
		time.Duration((*raftTimeout))*time.Millisecond,
		time.Duration((*raftPulse))*time.Millisecond,
	)
	if err != nil {
		return err
	}
	dir.ListenAndServe()
	return nil
}
