package main

import "flag"

type command struct {
	flag flag.FlagSet
	name string
	run  func()
}
