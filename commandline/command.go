package commandline

import "flag"

type Command struct {
	Name string
	Flag flag.FlagSet
	Run  func(args []string) error
}

var Commands = []*Command{
	DirCmd,
	StoreCmd,
}

var defaultConfig = `{
	"directory": [
		"127.0.0.1:9666"
	],
	"store": [
		"127.0.0.1:8666"
	]
}`
