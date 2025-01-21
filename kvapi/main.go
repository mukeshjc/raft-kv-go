package main

import (
	"bytes"
	crypto "crypto/rand"
	"encoding/binary"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"

	"github.com/mukeshjc/raft-kv-go/v2"
)

// To build on top of the Raft library we'll build, we need to create a state machine and commands that are sent to the state machine.
// Our state machine will have two operations: get a value from a key, and set a key to a value.
type stateMachine struct {
	db     *sync.Map
	server int
}

type commandKind uint8

const (
	setCommand commandKind = iota
	getCommand
)

type command struct {
	kind  commandKind
	key   string
	value string
}

// This is the hook that raft will call once the log entry is committed for each command (set or get) to update the final application state machine
func (s *stateMachine) Apply(cmd []byte) ([]byte, error) {
	c := decodeCommand(cmd)

	switch c.kind {
	case setCommand:
		s.db.Store(c.key, c.value)
	case getCommand:
		value, ok := s.db.Load(c.key)
		if !ok {
			return nil, fmt.Errorf("key not found")
		}
		return []byte(value.(string)), nil
	default:
		return nil, fmt.Errorf("unknown command: %x", cmd)
	}

	return nil, nil
}

// the Raft library we'll build needs to deal with various state machines. So commands passed from the user into the Raft cluster must be serialized to bytes.
func encodeCommand(cmd command) []byte {
	msg := bytes.NewBuffer(nil)
	err := msg.WriteByte(uint8(cmd.kind))
	if err != nil {
		panic(err)
	}

	err = binary.Write(msg, binary.LittleEndian, uint64(len(cmd.key)))
	if err != nil {
		panic(err)
	}

	msg.WriteString(cmd.key)

	err = binary.Write(msg, binary.LittleEndian, uint64(len(cmd.value)))
	if err != nil {
		panic(err)
	}

	msg.WriteString(cmd.value)

	return msg.Bytes()
}

func decodeCommand(cmd []byte) command {
	var c command

	// first byte is the command kind (set or get)
	c.kind = commandKind(cmd[0])

	keyLen := binary.LittleEndian.Uint64(cmd[1:9])
	c.key = string(cmd[9 : 9+keyLen])

	if c.kind == setCommand {
		valLen := binary.LittleEndian.Uint64(cmd[9+keyLen : 9+keyLen+8])
		c.value = string(cmd[9+keyLen+8 : 9+keyLen+8+valLen])
	}

	return c
}

type config struct {
	cluster []raft.ClusterMember
	index   int
	id      string
	address string
	http    string
}

func getConfig() config {
	cfg := config{}
	var node string
	for i, arg := range os.Args[1:] {
		if arg == "--node" {
			var err error
			node = os.Args[i+2]
			cfg.index, err = strconv.Atoi(node)
			if err != nil {
				log.Fatalf("expected $value to be a valid integer in `--node $value`, got: %s", node)
			}
			i++
			continue
		}

		if arg == "--http" {
			cfg.http = os.Args[i+2]
			i++
			continue
		}

		if arg == "--cluster" {
			cluster := os.Args[i+2]
			var clusterEntry raft.ClusterMember
			for _, part := range strings.Split(cluster, ";") {
				idAddress := strings.Split(part, ",")
				var err error
				clusterEntry.Id, err = strconv.ParseUint(idAddress[0], 10, 64)
				if err != nil {
					log.Fatalf("expected $id to be a valid integer in `--cluster $id,$ip`, got: %s", idAddress[0])
				}

				clusterEntry.Address = idAddress[1]
				cfg.cluster = append(cfg.cluster, clusterEntry)
			}

			i++
			continue
		}
	}

	if node == "" {
		log.Fatal("missing required parameter: --node $index")
	}

	if cfg.http == "" {
		log.Fatal("missing required parameter: --http $address")
	}

	if len(cfg.cluster) == 0 {
		log.Fatal("missing required parameter: --cluster $node1Id,$node1Address;...;$nodeNId,$nodeNAddress")
	}

	log.Printf("cfg looks like this : %v", cfg)
	return cfg
}

func main() {
	var b [8]byte
	_, err := crypto.Read(b[:])
	if err != nil {
		panic("cannot seed math/rand package with cryptographically secure random number generator")
	}
	rand.Seed(int64(binary.LittleEndian.Uint64(b[:])))

	cfg := getConfig()

	var db sync.Map

	var sm stateMachine
	sm.db = &db
	sm.server = cfg.index

	s := raft.NewServer(cfg.cluster, &sm, ".", cfg.index)
	s.Debug = true
	go s.Start()

	hs := httpServer{s, &db}

	http.HandleFunc("/set", hs.setHandler)
	http.HandleFunc("/get", hs.getHandler)
	err = http.ListenAndServe(cfg.http, nil)
	if err != nil {
		panic(err)
	}
}
