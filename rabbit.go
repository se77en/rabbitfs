package main

import (
	"flag"
	"fmt"
	"math/rand"
	"time"

	"code.google.com/p/log4go"
	"github.com/lilwulin/rabbitfs/commandline"
)

func main() {
	rand.Seed(time.Now().Unix())
	flag.Parse()
	args := flag.Args()
	if len(args) == 0 {
		printUsage()
		return
	}

	for _, c := range commandline.Commands {
		if args[0] == c.Name {
			if err := c.Flag.Parse(args[1:]); err != nil {
				log4go.Error("parse flag %s error", args[1:])
				return
			}
			args = c.Flag.Args()
			if err := c.Run(args); err != nil {
				fmt.Println(err.Error())
			}
			return
		}
	}

	// log.Errorf("command %s not found\n", args[0])
	printUsage()
}

func printUsage() {
	fmt.Printf("command:\n	rabbitfs directory\n	rabbitfs store\n")
}
