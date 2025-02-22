package main

import (
	"encoding/json"
	"log"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func main() {
	n := maelstrom.NewNode()
	var values []float64 = make([]float64, 0)

	n.Handle("broadcast", func(msg maelstrom.Message) error {
		var body map[string]any
		err := json.Unmarshal(msg.Body, &body)
		if err != nil {
			return err
		}
		var val float64 = body["message"].(float64)
		var f uint8 = 0
		for _, num := range values {
			if num == val {
				f = 1
				break
			}
		}
		if f == 0 {
			values = append(values, val)
			nodes := n.NodeIDs()
			for _, neighbour := range nodes {
				n.Send(neighbour, body)
			}
		}
		body["type"] = "broadcast_ok"
		delete(body, "message")
		return n.Reply(msg, body)
	})

	n.Handle("read", func(msg maelstrom.Message) error {
		var body map[string]any
		err := json.Unmarshal(msg.Body, &body)
		if err != nil {
			return err
		}

		body["type"] = "read_ok"
		body["messages"] = values

		return n.Reply(msg, body)
	})

	n.Handle("topology", func(msg maelstrom.Message) error {
		var body map[string]any
		err := json.Unmarshal(msg.Body, &body)
		if err != nil {
			return err
		}
		body["type"] = "topology_ok"
		delete(body, "topology")
		return n.Reply(msg, body)
	})

	err := n.Run()
	if err != nil {
		log.Fatal(err)
	}
}
