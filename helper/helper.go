package helper

import "os"

func DirRemover(dirs ...string) {
	for _, dir := range dirs {
		os.RemoveAll(dir)
	}
}
