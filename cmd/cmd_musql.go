package main

import (
	"github.com/frohmut/musql/internal"
	"log"
	"os"
)

func main() {
	err := run()
	if err != nil {
		log.Fatalf("%v", err)
	}
}

func run() error {
	var err error

	var args []string
	for i, v := range os.Args {
		if i != 0 {
			args = append(args, v)
		}
	}

	args = append(args, "-defini", "musql.ini")

	var m = &internal.Musql{}
	defer m.Close()

	var c = &internal.Config{}
	err = c.Parse(args)
	if err != nil {
		return err
	}
	err = c.Apply(m)
	if err != nil {
		return err
	}

	return nil
}
