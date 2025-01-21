package raft

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"path"
	"sync"
	"time"

	"github.com/mukeshjc/raft-kv-go/v2/utils"
)

const (
	PAGE_SIZE    = 4096
	ENTRY_HEADER = 16
	// ENTRY_SIZE is something that I could see being configurable based on the workload. Some workloads truly need only 128 bytes.\
	// But a key-value store probably wants much more than that. This implementation doesn't try to handle the case of completely arbitrary sized keys and values.
	ENTRY_SIZE = 128
)

type StateMachine interface {
	Apply(cmd []byte) ([]byte, error)
}

type ApplyResult struct {
	Result []byte
	Error  error
}

type Entry struct {
	Command []byte
	Term    uint64

	// Set by the primary so it can learn about the result of applying this command to the state machine
	result chan ApplyResult
}

type ClusterMember struct {
	Id      uint64
	Address string

	// Index of the next log entry to send
	nextIndex uint64
	// Highest log entry known to be replicated
	matchIndex uint64

	// Who was voted for in the most recent term
	votedFor uint64

	// TCP connection
	rpcClient *rpc.Client
}

type ServerState string

const (
	leaderState    ServerState = "leader"
	followerState  ServerState = "follower"
	candidateState ServerState = "candidate"
)

type Server struct {
	// These variables for shutting down.
	done   bool
	server *http.Server

	Debug bool

	mu sync.Mutex
	// ----------- PERSISTENT STATE -----------

	// The current term
	currentTerm uint64

	log []Entry

	// votedFor is stored in `cluster []ClusterMember` below,
	// mapped by `clusterIndex` below

	// ----------- READONLY STATE -----------

	// Unique identifier for this Server
	id uint64

	// The TCP address for RPC
	address string

	// When to start elections after no append entry messages
	electionTimeout time.Time

	// How often to send empty messages
	heartbeatMs int

	// When to next send empty message
	heartbeatTimeout time.Time

	// User-provided state machine
	stateMachine StateMachine

	// Metadata directory
	metadataDir string

	// Metadata store
	fd *os.File

	// ----------- VOLATILE STATE -----------

	// Index of highest log entry known to be committed
	commitIndex uint64

	// Index of highest log entry applied to state machine
	lastApplied uint64

	// Candidate, follower, or leader
	state ServerState

	// Servers in the cluster, including this one
	cluster []ClusterMember

	// Index of this server
	clusterIndex int
}

func NewServer(
	clusterConfig []ClusterMember, statemachine StateMachine,
	metadataDir string, clusterIndex int) *Server {
	// explicitly make a copy of the cluster because we'll be modifying it in this server.
	var cluster []ClusterMember
	cluster = append(cluster, clusterConfig...)

	return &Server{
		id:           cluster[clusterIndex].Id,
		address:      cluster[clusterIndex].Address,
		cluster:      cluster,
		stateMachine: statemachine,
		metadataDir:  metadataDir,
		clusterIndex: clusterIndex,
		heartbeatMs:  300,
		mu:           sync.Mutex{},
	}
}

func (s *Server) debugmsg(msg string) string {
	return fmt.Sprintf("%s [Id: %d, Term: %d] %s", time.Now().Format(time.RFC3339Nano), s.id, s.currentTerm, msg)
}

func (s *Server) debug(msg string) {
	if !s.Debug {
		return
	}
	fmt.Println(s.debugmsg(msg))
}

func (s *Server) debugf(msg string, args ...any) {
	if !s.Debug {
		return
	}

	s.debug(fmt.Sprintf(msg, args...))
}

// func (s *Server) warn(msg string) {
// 	fmt.Println("[WARN] " + s.debugmsg(msg))
// }

func (s *Server) warnf(msg string, args ...any) {
	fmt.Println(fmt.Sprintf(msg, args...))
}

func Server_assert[T comparable](s *Server, msg string, a, b T) {
	utils.Assert(s.debugmsg(msg), a, b)
}

// currentTerm, log, and votedFor must be persisted to disk as they're edited.
// simply binary encoding the fields and also, importantly, keeping track of exactly how many entries in the log must be written and only writing that many.
// Must be called within s.mu.Lock()
func (s *Server) persist(writeLog bool, nNewEntries int) {
	t := time.Now()

	if nNewEntries == 0 && writeLog {
		nNewEntries = len(s.log)
	}

	s.fd.Seek(0, 0)

	var page [PAGE_SIZE]byte
	// Bytes 0  - 8:   Current term
	// Bytes 8  - 16:  Voted for
	// Bytes 16 - 24:  Log length
	// Bytes 4096 - N: Log

	binary.LittleEndian.PutUint64(page[:8], s.currentTerm)
	binary.LittleEndian.PutUint64(page[8:16], s.getVotedFor())
	binary.LittleEndian.PutUint64(page[16:24], uint64(len(s.log)))
	n, err := s.fd.Write(page[:])
	if err != nil {
		panic(err)
	}
	Server_assert(s, "Wrote full page", n, PAGE_SIZE)

	if writeLog && nNewEntries > 0 {
		newLogOffset := max(len(s.log)-nNewEntries, 0)

		// we do that by seek-ing to the offset of the first entry that needs to be written.
		s.fd.Seek(int64(PAGE_SIZE+ENTRY_SIZE*newLogOffset), 0)
		bw := bufio.NewWriter(s.fd)

		var entryBytes [ENTRY_SIZE]byte
		for i := newLogOffset; i < len(s.log); i++ {
			// Bytes 0 - 8:    Entry term
			// Bytes 8 - 16:   Entry command length
			// Bytes 16 - ENTRY_SIZE: Entry command

			if len(s.log[i].Command) > ENTRY_SIZE-ENTRY_HEADER {
				panic(fmt.Sprintf("Command is too large (%d). Must be at most %d bytes.", len(s.log[i].Command), ENTRY_SIZE-ENTRY_HEADER))
			}

			binary.LittleEndian.PutUint64(entryBytes[:8], s.log[i].Term)
			binary.LittleEndian.PutUint64(entryBytes[8:16], uint64(len(s.log[i].Command)))
			copy(entryBytes[16:], []byte(s.log[i].Command))

			n, err := bw.Write(entryBytes[:])
			if err != nil {
				panic(err)
			}
			Server_assert(s, "Wrote full page", n, ENTRY_SIZE)
		}

		err = bw.Flush()
		if err != nil {
			panic(err)
		}
	}

	// design choice to call Sync here, because we are immediately writing to Disk to avoid loss of data if crash occurs
	if err = s.fd.Sync(); err != nil {
		panic(err)
	}
	s.debugf("Persisted in %s. Term: %d. Log Len: %d (%d new). Voted For: %d.", time.Since(t), s.currentTerm, len(s.log), nNewEntries, s.getVotedFor())
}

// must be called within s.mu.Lock()
func (s *Server) getVotedFor() uint64 {
	for i := range s.cluster {
		if i == s.clusterIndex {
			return s.cluster[i].votedFor
		}
	}

	Server_assert(s, "Invalid cluster", true, false)
	return 0
}

// restoring from disk. This will only be called once on startup.
func (s *Server) restore() {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.fd == nil {
		var err error
		s.fd, err = os.OpenFile(
			path.Join(s.metadataDir, fmt.Sprintf("md_%d.dat", s.id)),
			os.O_SYNC|os.O_CREATE|os.O_RDWR,
			0755)
		if err != nil {
			panic(err)
		}
	}

	s.fd.Seek(0, 0)

	// Bytes 0  - 8:   Current term
	// Bytes 8  - 16:  Voted for
	// Bytes 16 - 24:  Log length
	// Bytes 4096 - N: Log
	var page [PAGE_SIZE]byte
	n, err := s.fd.Read(page[:])
	if err == io.EOF {
		s.ensureLog()
		return
	} else if err != nil {
		panic(err)
	}
	Server_assert(s, "Read full page", n, PAGE_SIZE)

	s.currentTerm = binary.LittleEndian.Uint64(page[:8])
	s.setVotedFor(binary.LittleEndian.Uint64(page[8:16]))
	lenLog := binary.LittleEndian.Uint64(page[16:24])
	s.log = nil

	if lenLog > 0 {
		s.fd.Seek(int64(PAGE_SIZE), 0)

		var e Entry
		for i := 0; uint64(i) < lenLog; i++ {
			var entryBytes [ENTRY_SIZE]byte
			n, err := s.fd.Read(entryBytes[:])
			if err != nil {
				panic(err)
			}
			Server_assert(s, "Read full entry", n, ENTRY_SIZE)

			// Bytes 0 - 8:    Entry term
			// Bytes 8 - 16:   Entry command length
			// Bytes 16 - ENTRY_SIZE: Entry command
			e.Term = binary.LittleEndian.Uint64(entryBytes[:8])
			lenValue := binary.LittleEndian.Uint64(entryBytes[8:16])
			e.Command = entryBytes[16 : 16+lenValue]
			s.log = append(s.log, e)
		}
	}

	s.ensureLog()
}

func (s *Server) ensureLog() {
	if len(s.log) == 0 {
		// Always has at least one log entry.
		s.log = append(s.log, Entry{})
	}
}

// Must be called within s.mu.Lock()
func (s *Server) setVotedFor(id uint64) {
	for i := range s.cluster {
		if i == s.clusterIndex {
			s.cluster[i].votedFor = id
			return
		}
	}

	Server_assert(s, "Invalid cluster", true, false)
}

// the main loop. Before starting the loop we need to
// 1) restore persistent state from disk and
// 2) kick off an RPC server so servers in the cluster can send and receive messages to and from eachother.
// make sure rand is seeded
func (s *Server) Start() {
	s.mu.Lock()
	s.state = followerState
	s.done = false
	s.mu.Unlock()

	s.restore()

	rpcServer := rpc.NewServer()
	rpcServer.Register(s)
	l, err := net.Listen("tcp", s.address)
	if err != nil {
		panic(err)
	}
	mux := http.NewServeMux()
	mux.Handle(rpc.DefaultRPCPath, rpcServer)

	s.server = &http.Server{Handler: mux}
	go s.server.Serve(l)

	go func() {
		s.mu.Lock()
		s.resetElectionTimeout()
		s.mu.Unlock()

		for {
			s.mu.Lock()
			if s.done {
				s.mu.Unlock()
				return
			}
			state := s.state
			s.mu.Unlock()

			// In the main loop we are either in the leader state, follower state or candidate state.
			// All states will potentially receive RPC messages from other servers in the cluster but that won't be modeled in this main loop.

			// The only thing going on in the main loop is that:
			// We send heartbeat RPCs (leader state)
			// We try to advance the commit index (leader state only) and apply commands to the state machine (leader and follower states)
			// We trigger a new election if we haven't received a message in some time (candidate and follower states)
			// Or we become the leader (candidate state)
			switch state {
			case leaderState:
				s.heartbeat()
				s.advanceCommitIndex()
			case followerState:
				s.timeout()
				s.advanceCommitIndex()
			case candidateState:
				s.timeout()
				s.becomeLeader()
			}
		}
	}()
}

// Leader election
// Leader election happens every time nodes haven't received a message from a valid leader in some time.

// I'll break this up into four major pieces:
// Timing out and becoming a candidate after a random (but bounded) period of time of not hearing a message from a valid leader: s.timeout().
// The candidate requests votes from all other servers: s.requestVote().
// All servers handle vote requests: s.HandleRequestVoteRequest().
// A candidate with a quorum of vote requests becomes the leader: s.becomeLeader().

// You increment currentTerm, vote for yourself and send RPC vote requests to other nodes in the server.

func (s *Server) resetElectionTimeout() {
	interval := time.Duration(rand.Intn(s.heartbeatMs*2) + s.heartbeatMs*2)
	s.debugf("New interval: %s.", interval*time.Millisecond)
	s.electionTimeout = time.Now().Add(interval * time.Millisecond)
}

func (s *Server) timeout() {
	s.mu.Lock()
	defer s.mu.Unlock()

	hasTimedOut := time.Now().After(s.electionTimeout)
	if hasTimedOut {
		s.debug("Timed out, starting new election.")
		s.state = candidateState
		s.currentTerm++
		for i := range s.cluster {
			if i == s.clusterIndex {
				s.cluster[i].votedFor = s.id
			} else {
				s.cluster[i].votedFor = 0
			}
		}

		s.resetElectionTimeout()
		s.persist(false, 0)
		s.requestVote()
	}
}

// fill the RequestVoteRequest struct out and send it to each other node in the cluster in parallel.
// As we iterate through nodes in the cluster, we skip ourselves (we always immediately vote for ourselves).
func (s *Server) requestVote() {
	for i := range s.cluster {
		if i == s.clusterIndex {
			continue
		}

		go func(i int) {
			s.mu.Lock()

			s.debugf("Requesting vote from %d.", s.cluster[i].Id)

			lastLogIndex := uint64(len(s.log) - 1)
			lastLogTerm := s.log[len(s.log)-1].Term

			req := RequestVoteRequest{
				RPCMessage: RPCMessage{
					Term: s.currentTerm,
				},
				CandidateId:  s.id,
				LastLogIndex: lastLogIndex,
				LastLogTerm:  lastLogTerm,
			}
			s.mu.Unlock()

			var rsp RequestVoteResponse
			ok := s.rpcCall(i, "Server.HandleRequestVoteRequest", req, &rsp)
			if !ok {
				// Will retry later
				return
			}

			// Figure 2 in the Raft paper that we must always check that the RPC request and response is still valid. If the term of the response is greater than our own term,
			// we must immediately stop processing and revert to follower state.

			// Otherwise only if the response is still relevant to us at the moment (the response term is the same as the request term) and the request has succeeded do we count the vote.

			s.mu.Lock()
			defer s.mu.Unlock()

			if s.updateTerm(rsp.RPCMessage) {
				return
			}

			dropStaleResponse := rsp.Term != req.Term
			if dropStaleResponse {
				return
			}

			if rsp.VoteGranted {
				s.debugf("Vote granted by %d.", s.cluster[i].Id)
				s.cluster[i].votedFor = s.id
			}
		}(i)
	}
}

// implementation of s.updateTerm() is simple. It just takes care of transitioning to follower state if the term of an RPC message is greater than the node's current term.
// must be called within a s.mu.Lock()
func (s *Server) updateTerm(msg RPCMessage) bool {
	transitioned := false
	if msg.Term > s.currentTerm {
		s.currentTerm = msg.Term
		s.state = followerState
		s.setVotedFor(0)
		transitioned = true
		s.debug("Transitioned to follower")
		s.resetElectionTimeout()
		s.persist(false, 0)
	}
	return transitioned
}

// the implementation of s.rpcCall() is a wrapper around net/rpc to lazily connect.
func (s *Server) rpcCall(i int, name string, req, rsp any) bool {
	s.mu.Lock()
	c := s.cluster[i]
	var err error
	var rpcClient *rpc.Client = c.rpcClient
	if c.rpcClient == nil {
		c.rpcClient, err = rpc.DialHTTP("tcp", c.Address)
		rpcClient = c.rpcClient
	}
	s.mu.Unlock()

	// TODO: where/how to reconnect if the connection must be reestablished?

	if err == nil {
		err = rpcClient.Call(name, req, rsp)
	}

	if err != nil {
		s.warnf("Error calling %s on %d: %s.", name, c.Id, err)
	}

	return err == nil
}

// what happens when a node receives a vote request?
// we must always check the RPC term versus our own and revert to follower if the term is greater than our own.
// (Remember that since this is an RPC request it could come to a server in any state: leader, candidate, or follower.)
// this is automatically accepted when registering the RPC at line 360
func (s *Server) HandleRequestVoteRequest(req RequestVoteRequest, rsp *RequestVoteResponse) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.updateTerm(req.RPCMessage)

	s.debugf("Received vote request from %d.", req.CandidateId)

	// then we can return immediately if the request term is lower than our own (that means it's an old request).
	rsp.VoteGranted = false
	rsp.Term = s.currentTerm

	if req.Term < s.currentTerm {
		s.debugf("Not granting vote request from %d.", req.CandidateId)
		Server_assert(s, "VoteGranted = false", rsp.VoteGranted, false)
		return nil
	}

	// finally, we check to make sure the requester's log is at least as up-to-date as our own and that we haven't already voted for ourselves.
	// The first condition (up-to-date log) was not described in the Raft paper that I could find. But the author of the paper published a Raft TLA+ spec that does have it defined.
	// And the second condition you might think could never happen since we already wrote the code that said when we trigger an election we vote for ourselves. But since each server has a random election timeout,
	// the one who starts the election will differ in timing sufficiently enough to catch other servers and allow them to vote for it.

	lastLogTerm := s.log[len(s.log)-1].Term
	logLen := uint64(len(s.log) - 1)
	logOk := req.LastLogTerm > lastLogTerm ||
		(req.LastLogTerm == lastLogTerm && req.LastLogIndex >= logLen)
	grant := req.Term == s.currentTerm &&
		logOk &&
		(s.getVotedFor() == 0 || s.getVotedFor() == req.CandidateId)
	if grant {
		s.debugf("Voted for %d.", req.CandidateId)
		s.setVotedFor(req.CandidateId)
		rsp.VoteGranted = true
		s.resetElectionTimeout()
		s.persist(false, 0)
	} else {
		s.debugf("Not granting vote request from %d.", +req.CandidateId)
	}

	return nil
}

// we need to address how the candidate who sent out vote requests actually becomes the leader.
// simple - If we have a quorum of votes, we become the leader!
func (s *Server) becomeLeader() {
	s.mu.Lock()
	defer s.mu.Unlock()

	quorum := len(s.cluster)/2 + 1
	for i := range s.cluster {
		if s.cluster[i].votedFor == s.id && quorum > 0 {
			quorum--
		}
	}

	// there is a bit of bookkeeping we need to do like resetting nextIndex and matchIndex for each server (noted in Figure 2).
	// And we also need to append a blank entry for the new term.
	if quorum == 0 {
		// Reset all cluster state
		for i := range s.cluster {
			s.cluster[i].nextIndex = uint64(len(s.log) + 1)
			// Yes, even matchIndex is reset. Figure 2
			// from Raft shows both nextIndex and
			// matchIndex are reset after every election.
			s.cluster[i].matchIndex = 0
		}

		s.debug("New leader.")
		s.state = leaderState

		// From Section 8 Client Interaction:
		// > First, a leader must have the latest information on
		// > which entries are committed. The Leader
		// > Completeness Property guarantees that a leader has
		// > all committed entries, but at the start of its
		// > term, it may not know which those are. To find out,
		// > it needs to commit an entry from its term. Raft
		// > handles this by having each leader commit a blank
		// > no-op entry into the log at the start of its term.
		s.log = append(s.log, Entry{Term: s.currentTerm, Command: nil})
		s.persist(true, 1)

		// Triggers s.appendEntries() in the next tick of the
		// main state loop.
		s.heartbeatTimeout = time.Now()
	}
}

// Log replication
// I'll break up log replication into four major pieces:

// User submits a message to the leader to be replicated: s.Apply().
// The leader sends uncommitted messages (messages from nextIndex) to all followers: s.appendEntries().
// A follower receives a AppendEntriesRequest and stores new messages if appropriate, letting the leader know when it does store the messages: s.HandleAppendEntriesRequest().
// The leader tries to update commitIndex for the last uncommitted message by seeing if it's been replicated on a quorum of servers: s.advanceCommitIndex().

// s.Apply() : is the entry point for a user of the cluster to attempt to get messages replicated into the cluster.
// It must be called on the current leader of the cluster. In the future the failure response might include the current leader. Or the user could submit messages in parallel to all nodes in the cluster and ignore ErrApplyToLeader.
// In the meantime we just assume the user can figure out which server in the cluster is the leader.
var ErrApplyToLeader = errors.New("cannot apply message to follower, apply to leader")

func (s *Server) Apply(commands [][]byte) ([]ApplyResult, error) {
	s.mu.Lock()
	if s.state != leaderState {
		s.mu.Unlock()
		return nil, ErrApplyToLeader
	}
	s.debugf("Processing %d new entry!", len(commands))

	// we'll store the message in the leader's log along with a Go channel that we must block on for the result of applying the message in the state machine after the message has been committed to the cluster.
	resultChans := make([]chan ApplyResult, len(commands))
	for i, command := range commands {
		resultChans[i] = make(chan ApplyResult)
		s.log = append(s.log, Entry{
			Term:    s.currentTerm,
			Command: command,
			result:  resultChans[i],
		})
	}

	s.persist(true, len(commands))

	// we kick off the replication process (this will not block).
	s.debug("Waiting to be applied!")
	s.mu.Unlock()

	// interesting thing here is that appending entries is detached from the messages we just received. s.appendEntries() will probably include at least the messages we just appended to our log, but it might include more too if some servers are not very up-to-date.
	// It may even include less than the messages we append to our log since we'll restrict the number of entries to send at one time so we keep latency down.
	s.appendEntries()

	// then we block until we receive results from each of the channels we created.
	// TODO: What happens if this takes too long?
	results := make([]ApplyResult, len(commands))
	var wg sync.WaitGroup
	wg.Add(len(commands))
	for i, ch := range resultChans {
		go func(i int, c chan ApplyResult) {
			results[i] = <-c
			wg.Done()
		}(i, ch)
	}

	wg.Wait()

	return results, nil
}

// This is the meat of log replication on the leader side. We send unreplicated messages to each other server in the cluster.
// the method itself, we start optimistically sending no entries and decrement nextIndex for each server as the server fails to replicate messages. This means that we might eventually end up sending the entire log to one or all servers.
// We'll set a max number of entries to send per request so we avoid unbounded latency as followers store entries to disk. But we still want to send a large batch so that we amortize the cost of fsync.
const MAX_APPEND_ENTRIES_BATCH = 8_000

func (s *Server) appendEntries() {
	for i := range s.cluster {
		// Don't need to send message to self
		if i == s.clusterIndex {
			continue
		}

		go func(i int) {
			s.mu.Lock()

			next := s.cluster[i].nextIndex
			prevLogIndex := next - 1
			prevLogTerm := s.log[prevLogIndex].Term

			var entries []Entry
			if uint64(len(s.log)-1) >= s.cluster[i].nextIndex {
				s.debugf("len: %d, next: %d, server: %d", len(s.log), next, s.cluster[i].Id)
				entries = s.log[next:]
			}

			// Keep latency down by only applying N at a time.
			if len(entries) > MAX_APPEND_ENTRIES_BATCH {
				entries = entries[:MAX_APPEND_ENTRIES_BATCH]
			}

			lenEntries := uint64(len(entries))
			req := AppendEntriesRequest{
				RPCMessage: RPCMessage{
					Term: s.currentTerm,
				},
				LeaderId:     s.cluster[s.clusterIndex].Id,
				PrevLogIndex: prevLogIndex,
				PrevLogTerm:  prevLogTerm,
				Entries:      entries,
				LeaderCommit: s.commitIndex,
			}

			s.mu.Unlock()

			var rsp AppendEntriesResponse
			s.debugf("Sending %d entries to %d for term %d.", len(entries), s.cluster[i].Id, req.Term)
			ok := s.rpcCall(i, "Server.HandleAppendEntriesRequest", req, &rsp)
			if !ok {
				// Will retry next tick
				return
			}

			// as with every RPC request and response, we must check terms and potentially drop the message if it's outdated.
			s.mu.Lock()
			defer s.mu.Unlock()
			if s.updateTerm(rsp.RPCMessage) {
				return
			}

			dropStaleResponse := rsp.Term != req.Term && s.state == leaderState
			if dropStaleResponse {
				return
			}

			// Otherwise, if the message was successful, we'll update matchIndex (the last confirmed message stored on the follower) and nextIndex (the next likely message to send to the follower).
			// If the message was not successful, we decrement nextIndex. Next time s.appendEntries() is called it will include one more previous message for this replica.
			if rsp.Success {
				prev := s.cluster[i].nextIndex
				s.cluster[i].nextIndex = max(req.PrevLogIndex+lenEntries+1, 1)
				s.cluster[i].matchIndex = s.cluster[i].nextIndex - 1
				s.debugf("Message accepted for %d. Prev Index: %d, Next Index: %d, Match Index: %d.", s.cluster[i].Id, prev, s.cluster[i].nextIndex, s.cluster[i].matchIndex)
			} else {
				s.cluster[i].nextIndex = max(s.cluster[i].nextIndex-1, 1)
				s.debugf("Forced to go back to %d for: %d.", s.cluster[i].nextIndex, s.cluster[i].Id)
			}
		}(i)
	}
} // we're done the leader side of append entries!

// Now for the follower side of log replication.
// This is, again, an RPC handler that could be called at any moment. So we need to potentially update the term (and transition to follower).
func (s *Server) HandleAppendEntriesRequest(req AppendEntriesRequest, rsp *AppendEntriesResponse) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.updateTerm(req.RPCMessage)

	// From Candidates (§5.2) in Figure 2
	// If AppendEntries RPC received from new leader: convert to follower
	if req.Term == s.currentTerm && s.state == candidateState {
		s.state = followerState
	}

	rsp.Term = s.currentTerm
	rsp.Success = false

	if s.state != followerState {
		s.debugf("Non-follower cannot append entries.")
		return nil
	}

	// we also return early if the request term is less than our own. This would represent an old request.
	if req.Term < s.currentTerm {
		s.debugf("Dropping request from old leader %d: term %d.", req.LeaderId, req.Term)
		// Not a valid leader.
		return nil
	}

	// finally, we know we're receiving a request from a valid leader. So we need to immediately bump the election timeout.
	s.resetElectionTimeout()

	// we do the log comparison to see if we can add the entries sent from this request.
	// Specifically, we make sure that our log at req.PrevLogIndex exists and has the same term as req.PrevLogTerm.
	logLen := uint64(len(s.log))
	validPreviousLog := req.PrevLogIndex == 0 /* This is the induction step */ ||
		(req.PrevLogIndex < logLen &&
			s.log[req.PrevLogIndex].Term == req.PrevLogTerm)
	if !validPreviousLog {
		s.debug("Not a valid log.")
		return nil
	}

	// Next, we've got valid entries that we need to add to our log. This implementation is a little more complex because we'll make use of Go slice capacity so that append() never allocates.
	// Importantly, we must truncate the log if a new entry ever conflicts with an existing one:
	//   If an existing entry conflicts with a new one (same index but different terms), delete the existing entry and all that follow it (§5.3)
	next := req.PrevLogIndex + 1
	nNewEntries := 0

	for i := next; i < next+uint64(len(req.Entries)); i++ {
		e := req.Entries[i-next]
		if i >= uint64(cap(s.log)) {
			newTotal := next + uint64(len(req.Entries))
			// Second argument must actually be `i`
			// not `0` otherwise the copy after this
			// doesn't work.
			// Only copy until `i`, not `newTotal` since
			// we'll continue appending after this.
			newLog := make([]Entry, i, newTotal*2)
			copy(newLog, s.log)
			s.log = newLog
		}

		if i < uint64(len(s.log)) && s.log[i].Term != e.Term {
			prevCap := cap(s.log)
			// If an existing entry conflicts with a new
			// one (same index but different terms),
			// delete the existing entry and all that
			// follow it (§5.3)
			s.log = s.log[:i]
			Server_assert(s, "Capacity remains the same while we truncated.", cap(s.log), prevCap)
		}

		s.debugf("Appending entry: %s. At index: %d.", string(e.Command), len(s.log))

		if i < uint64(len(s.log)) {
			Server_assert(s, "Existing log is the same as new log", s.log[i].Term, e.Term)
		} else {
			s.log = append(s.log, e)
			Server_assert(s, "Length is directly related to the index.", uint64(len(s.log)), i+1)
			nNewEntries++
		}
	}

	// Finally, we update the server's local commitIndex to the min of req.LeaderCommit and our own log length.
	// And finally we persist all these changes and mark the response as successful.
	if req.LeaderCommit > s.commitIndex {
		s.commitIndex = min(req.LeaderCommit, uint64(len(s.log)-1))
	}

	s.persist(nNewEntries != 0, nNewEntries)

	rsp.Success = true
	return nil

	// So the combined behavior of the leader and follower when replicating is that a follower not in sync with the leader may eventually
	// go down to the beginning of the log so the leader and follower have some first N messages of the log that match.
}

// Now when not just one follower but a quorum of followers all have a matching first N messages, the leader can advance the cluster's commitIndex.
func (s *Server) advanceCommitIndex() {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Leader can update commitIndex on quorum.
	if s.state == leaderState {
		lastLogIndex := uint64(len(s.log) - 1)

		for i := lastLogIndex; i > s.commitIndex; i-- {
			quorum := len(s.cluster)/2 + 1

			for j := range s.cluster {
				if quorum == 0 {
					break
				}

				isLeader := j == s.clusterIndex
				if s.cluster[j].matchIndex >= i || isLeader {
					quorum--
				}
			}

			if quorum == 0 {
				s.commitIndex = i
				s.debugf("New commit index: %d.", i)
				break
			}
		}
	}

	// And for every state a server might be in, if there are messages committed but not applied, we'll apply one here.
	// And importantly, we'll pass the result back to the message's result channel if it exists, so that s.Apply() can learn about the result.
	if s.lastApplied <= s.commitIndex {
		log := s.log[s.lastApplied]

		// len(log.Command) == 0 is a noop committed by the leader.
		if len(log.Command) != 0 {
			s.debugf("Entry applied: %d.", s.lastApplied)
			// TODO: what if Apply() takes too long?
			res, err := s.stateMachine.Apply(log.Command)

			// Will be nil for follower entries and for no-op entries.
			// Not nil for all user submitted messages.
			if log.result != nil {
				log.result <- ApplyResult{
					Result: res,
					Error:  err,
				}
			}
		}

		s.lastApplied++
	}
}

// Heartbeats combine log replication and leader election. Heartbeats stave off leader election (follower timeouts). And heartbeats also bring followers up-to-date if they are behind.
// And it's a simple method. If it's time to heartbeat, we call s.appendEntries(). That's it.
// The reason this staves off leader election is because any number of entries (0 or N) will come from a valid leader and will thus cause the followers to reset their election timeout.
func (s *Server) heartbeat() {
	s.mu.Lock()
	defer s.mu.Unlock()

	timeForHeartbeat := time.Now().After(s.heartbeatTimeout)
	if timeForHeartbeat {
		s.heartbeatTimeout = time.Now().Add(time.Duration(s.heartbeatMs) * time.Millisecond)
		s.debug("Sending heartbeat")
		s.appendEntries()
	}
}
