package main

import (
	"encoding/json"
	"log"

	"github.com/google/uuid"
	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func main() {
	node := maelstrom.NewNode()
	node.Handle("generate", func(msg maelstrom.Message) error {
		var body map[string]any
		err := json.Unmarshal(msg.Body, &body)
		if err != nil {
			return err
		}
		id := uuid.New()
		body["type"] = "generate_ok"
		body["id"] = id.ID()
		return node.Reply(msg, body)
	})
	if err := node.Run(); err != nil {
		log.Fatal(err)
	}
}
