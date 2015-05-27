package main

import (
	"flag"

	"github.com/lilwulin/rabbitfs/log"
)

var commands = []*command{
	masterCommand,
}

func main() {
	flag.Parse()
	args := flag.Args()
	if len(args) == 0 {
		printUsage()
		return
	}

	for _, c := range commands {
		if args[0] == c.name {
			if err := c.flag.Parse(args[1:]); err != nil {
				log.Fatalf("parse flag %s error", args[1:])
			}
			c.run()
			return
		}
	}

	log.Errorf("command %s not found\n", args[0])
	printUsage()
}

func printUsage() {
	// TODO: fill this function to print usage for users
}
