package main

import (
	"encoding/json"
	"log"
	"time"

	"slices"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func main() {
	n := maelstrom.NewNode()
	var values []float64

	n.Handle("broadcast", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		val := body["message"].(float64)
		if !slices.Contains(values, val) {
			values = append(values, val)
			nodes := n.NodeIDs()
			for _, neighbour := range nodes {
				if neighbour != n.ID() && neighbour != msg.Src {
					go broadcast(10*time.Millisecond, n, neighbour, body)
				}
			}
		}

		reply := map[string]any{"type": "broadcast_ok"}
		return n.Reply(msg, reply)
	})

	n.Handle("read", func(msg maelstrom.Message) error {
		reply := map[string]any{
			"type":     "read_ok",
			"messages": values,
		}
		return n.Reply(msg, reply)
	})

	n.Handle("topology", func(msg maelstrom.Message) error {
		reply := map[string]any{"type": "topology_ok"}
		return n.Reply(msg, reply)
	})

	n.Handle("broadcast_ok", func(msg maelstrom.Message) error {
		return nil
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}

func broadcast(backoff time.Duration, node *maelstrom.Node, dest string, body map[string]any) {
	replied := false
	node.RPC(dest, body, func(msg maelstrom.Message) error {
		replied = true
		return nil
	})
	time.Sleep(backoff)
	if !replied {
		broadcast(backoff*2, node, dest, body)
	}
}
