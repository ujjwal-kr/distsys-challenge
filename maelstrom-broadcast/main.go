package main

import (
	"encoding/json"
	"log"
	"sync"

	"slices"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func main() {
	n := maelstrom.NewNode()
	var values []float64 = make([]float64, 0)
	var leftover []map[string]any
	n.Handle("broadcast", func(msg maelstrom.Message) error {
		var body map[string]any
		err := json.Unmarshal(msg.Body, &body)
		if err != nil {
			return err
		}
		var val float64 = body["message"].(float64)
		var f uint8 = 0
		if slices.Contains(values, val) {
			f = 1
		}
		if f == 0 {
			values = append(values, val)
			nodes := n.NodeIDs()
			var wg1 sync.WaitGroup
			wg1.Add(len(nodes))
			for _, neighbour := range nodes {
				go func(neighbour string) {
					n.RPC(neighbour, body, func(msg maelstrom.Message) error {
						var response map[string]any
						err := json.Unmarshal(msg.Body, &response)
						if response["type"] == "error" {
							leftover = append(leftover, body)
						}
						wg1.Done()
						return err
					})
				}(neighbour)
			}
			var wg2 sync.WaitGroup
			wg2.Add(len(leftover))

			for _, msg := range leftover {
				go func(msg map[string]any) {
					for _, neighbour := range nodes {
						n.Send(neighbour, msg)
					}
					wg2.Done()
				}(msg)
			}
			wg1.Wait()
			wg2.Wait()
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
