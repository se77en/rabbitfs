package log

import (
	"fmt"

	"github.com/golang/glog"
)

// Infoln wraps glog's V.Infoln
func Infoln(v glog.Level, args ...interface{}) {
	fmt.Println(args)
	glog.V(v).Infoln(args)
}

// Infof wraps glog's V.Infof
func Infof(v glog.Level, format string, args ...interface{}) {
	fmt.Printf(format, args)
	glog.V(v).Infof(format, args)
}

// Errorf wraps glog's Errorf
func Errorf(format string, args ...interface{}) {
	fmt.Printf(format, args)
	glog.Errorf(format, args)
}

// Errorln wraps glog's Errorln
func Errorln(args ...interface{}) {
	fmt.Println(args)
	glog.Errorln(args)
}

// Fatalf wraps glog's Fatalf
func Fatalf(format string, args ...interface{}) {
	fmt.Printf(format, args)
	glog.Fatalf(format, args)
}

// Fatalln wraps glog's Fatalln
func Fatalln(args ...interface{}) {
	fmt.Println(args)
	glog.Fatalln(args)
}
