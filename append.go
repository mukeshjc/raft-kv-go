package raft

type AppendEntriesRequest struct {
	RPCMessage

	// So follower can redirect clients
	LeaderId uint64

	// Index of log entry immediately preceding new ones
	PrevLogIndex uint64

	// Term of prevLogIndex entry
	PrevLogTerm uint64

	// Log entries to store. Empty for heartbeat.
	Entries []Entry

	// Leader's commitIndex
	LeaderCommit uint64
}

type AppendEntriesResponse struct {
	RPCMessage

	// true if follower contained entry matching prevLogIndex and
	// prevLogTerm
	Success bool
}
