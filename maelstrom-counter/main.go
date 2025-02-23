package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func main() {
	n := maelstrom.NewNode()
	kv := maelstrom.NewSeqKV(n)
	ctx := context.Background()

	var count int64 = 0

	n.Handle("read", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		value, err := kv.Read(ctx, fmt.Sprint(count))
		if err != nil {
			return err
		}
		body["type"] = "read_ok"
		body["value"] = value.(float64)
		return n.Reply(msg, body)
	})

	n.Handle("add", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		var value float64 = body["delta"].(float64)
		latest, err := kv.Read(ctx, fmt.Sprint(count))
		if err != nil {
			return err
		}
		if value != latest {
			kv.Write(ctx, fmt.Sprint(count), value)
			count++
			for _, node := range n.NodeIDs() {
				if node != n.ID() {
					n.Send(node, body)
				}
			}
		}
		delete(body, "delta")
		body["type"] = "add_ok"
		return n.Reply(msg, body)
	})

	err := n.Run()
	if err != nil {
		log.Fatal(err)
	}
}
