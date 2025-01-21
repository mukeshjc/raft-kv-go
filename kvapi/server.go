package main

import (
	"fmt"
	"log"
	"net/http"
	"sync"

	"github.com/mukeshjc/raft-kv-go/v2"
)

type httpServer struct {
	raft *raft.Server
	db   *sync.Map
}

// we tell the Raft cluster we want this message replicated. The message contains the operation type (set) and the operation details (key and value).
// These messages are custom to the state machine we wrote. And they will be interpreted by the state machine we wrote, on each node in the cluster.
// Example:
// curl http://localhost:2020/set?key=x&value=1
func (hs httpServer) setHandler(w http.ResponseWriter, r *http.Request) {
	var c command
	c.kind = setCommand
	c.key = r.URL.Query().Get("key")
	c.value = r.URL.Query().Get("value")

	_, err := hs.raft.Apply([][]byte{encodeCommand(c)})
	if err != nil {
		log.Printf("could not write key value: %s", err)
		http.Error(w, http.StatusText(http.StatusBadRequest), http.StatusBadRequest)
		return
	}
}

// we handle get-ing values from the cluster. There are two ways to do this. We already embed a local copy of the distributed key-value map. We could just read from that map in the current process. But it might not be up-to-date or correct. It would be fast to read though. And convenient for debugging.
// But the only correct way to read from a Raft cluster [https://github.com/etcd-io/etcd/issues/741] is to pass the read through the log replication too.
// So we'll support both.
// Example:
// curl http://localhost:2020/set?key=x
// curl http://localhost:2020/get?key=x&relaxed=true # Skips consensus for the read.
func (hs httpServer) getHandler(w http.ResponseWriter, r *http.Request) {
	var c command
	c.kind = getCommand
	c.key = r.URL.Query().Get("key")

	var value []byte
	var err error
	if r.URL.Query().Get("relaxed") == "true" {
		v, ok := hs.db.Load(c.key)
		if !ok {
			err = fmt.Errorf("key not found in the local copy of Map in the HTTP server")
		} else {
			value = []byte(v.(string))
		}
	} else {
		var results []raft.ApplyResult
		results, err = hs.raft.Apply([][]byte{encodeCommand(c)})
		if err == nil {
			if len(results) != 1 {
				err = fmt.Errorf("expected single response from raft, got %d", len(results))
			} else if results[0].Error != nil {
				err = results[0].Error
			} else {
				value = results[0].Result
			}
		}

		if err != nil {
			log.Printf("Could not encode key-value in http response: %s", err)
			http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
			return
		}

		written := 0
		for written < len(value) {
			n, err := w.Write(value[written:])
			if err != nil {
				log.Printf("could not encode key-value in http response: %s", err)
				http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
				return
			}
			written += n
		}

	}
}
