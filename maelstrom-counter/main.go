package main

import (
	"log"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func main() {
	n := maelstrom.NewNode()

	err := n.Run()
	if err != nil {
		log.Fatal(err)
	}
}
