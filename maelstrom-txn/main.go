package main

import (
	"context"
	"encoding/json"
	"log"

	"fmt"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func main() {
	n := maelstrom.NewNode()
	kv := maelstrom.NewLWWKV(n)
	ctx := context.Background()

	n.Handle("txn", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		var txnMaps [][]any = make([][]any, 0)
		for _, rawOps := range body["txn"].([]any) {
			var ops []any = make([]any, 0)
			for _, item := range rawOps.([]any) {
				ops = append(ops, item)
			}
			opcode := ops[0].(string)
			arg1 := fmt.Sprint(int(ops[1].(float64)))
			switch opcode {
			case "r":
				val, _ := kv.ReadInt(ctx, arg1)
				s := []any{"r", int(ops[1].(float64)), val}
				txnMaps = append(txnMaps, s)
			case "w":
				arg2 := fmt.Sprint(int(ops[2].(float64)))
				kv.Write(ctx, arg1, arg2)
				txnMaps = append(txnMaps, ops)
			}
		}
		body["type"] = "txn_ok"
		body["txn"] = txnMaps
		return n.Reply(msg, body)
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
