package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"bytes"
	"encoding/gob"
	"math/rand"
	"sync"
	"time"
)
import "dissys_raft/labrpc"

// import "bytes"
// import "encoding/gob"

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

type LogEntry struct {
	Index   int         // Index of this log entry in the log
	Term    int         // Term when the entry was received by the leader
	Command interface{} // Command for the state machine
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex
	peers     []*labrpc.ClientEnd
	persister *Persister
	me        int // index into peers[]

	// Your data here.
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentTerm int        // Latest term server has seen
	votedFor    int        // CandidateId that received vote in current term (-1 if none)
	log         []LogEntry // Log entries

	state     string    // "Follower", "Candidate", "Leader"
	lastHeard time.Time // Last time a heartbeat or valid RPC was received

	applyCh         chan ApplyMsg // Channel to send committed log entries
	electionTimeout time.Duration

	commitIndex int // Index of highest log entry known to be committed
	lastApplied int // Index of highest log entry applied to state machine

	nextIndex  []int // 每个 Follower 下一个需要发送日志的索引
	matchIndex []int // 每个 Follower 已经匹配的最高日志索引
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	term = rf.currentTerm
	isleader = rf.state == "Leader"
	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() {
	// Your code here.
	// Example:
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	// Your code here.
	// Example:
	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)
	d.Decode(&rf.currentTerm)
	d.Decode(&rf.votedFor)
	d.Decode(&rf.log)
}

// example RequestVote RPC arguments structure.
type RequestVoteArgs struct {
	Term         int // Candidate's current term
	CandidateId  int // Candidate requesting the vote (its index in peers[])
	LastLogIndex int // Index of candidate's last log entry
	LastLogTerm  int // Term of candidate's last log entry
}

// example RequestVote RPC reply structure.
type RequestVoteReply struct {
	Term        int  // CurrentTerm, for candidate to update itself if outdated
	VoteGranted bool // True if candidate received the vote
}

type AppendEntriesArgs struct {
	Term         int        // Leader's term
	LeaderId     int        // Leader's ID
	PrevLogIndex int        // Index of log entry immediately preceding new entries
	PrevLogTerm  int        // Term of `PrevLogIndex` entry
	Entries      []LogEntry // Log entries to store (empty for heartbeat)
	LeaderCommit int        // Leader's commit index
}

type AppendEntriesReply struct {
	Term    int  // Current term, for leader to update itself
	Success bool // True if follower contained entry matching `PrevLogIndex` and `PrevLogTerm`
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		rf.persist()
		return
	}

	// If term is newer, update to follower
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.state = "Follower"
		rf.votedFor = -1
		rf.persist()
	}
	lastLog := rf.log[len(rf.log)-1]
	upToDate := args.LastLogTerm > lastLog.Term || (args.LastLogTerm == lastLog.Term && args.LastLogIndex >= lastLog.Index)
	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && upToDate {
		rf.votedFor = args.CandidateId
		rf.lastHeard = time.Now()
		rf.persist()
		reply.VoteGranted = true
	} else {
		reply.VoteGranted = false
	}

	reply.Term = rf.currentTerm
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// returns true if labrpc says the RPC was delivered.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// 如果当前节点不是 Leader，直接返回
	if rf.state != "Leader" {
		return -1, rf.currentTerm, false
	}

	// 创建新的日志条目并追加到日志中
	index := len(rf.log)
	term := rf.currentTerm
	newEntry := LogEntry{
		Index:   index,
		Term:    term,
		Command: command,
	}
	rf.log = append(rf.log, newEntry)
	rf.persist()
	// 异步发送日志到 followers
	go rf.sendHeartbeats()

	return index, term, true
}

// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (rf *Raft) Kill() {
	// Your code here, if desired.
}

func (rf *Raft) runElectionTimer() {
	for {
		time.Sleep(10 * time.Millisecond)
		rf.mu.Lock()
		if time.Since(rf.lastHeard) > rf.electionTimeout && rf.state != "Leader" {
			rf.startElection()
		}
		rf.mu.Unlock()
	}
}

func (rf *Raft) startElection() {
	rf.state = "Candidate"
	rf.currentTerm++
	rf.persist()
	rf.votedFor = rf.me
	rf.persist()
	rf.lastHeard = time.Now()
	args := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: len(rf.log) - 1,
		LastLogTerm:  rf.log[len(rf.log)-1].Term,
	}
	voteCount := 1
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go func(i int) {
			reply := RequestVoteReply{}
			if rf.sendRequestVote(i, args, &reply) {
				rf.mu.Lock()
				defer rf.mu.Unlock()

				if reply.Term > rf.currentTerm {
					rf.currentTerm = reply.Term
					rf.state = "Follower"
					rf.votedFor = -1
					rf.persist()
					return
				}

				if reply.VoteGranted && rf.state == "Candidate" {
					voteCount++
					if voteCount > len(rf.peers)/2 {
						rf.state = "Leader"
						rf.initializeLeaderState()
					}
				}
			}
		}(i)
	}
}

func (rf *Raft) initializeLeaderState() {
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	for i := range rf.peers {
		rf.nextIndex[i] = len(rf.log)
		rf.matchIndex[i] = 0
	}
}

func (rf *Raft) sendAppendEntries(server int, args AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) runHeartbeatTimer() {
	for {
		time.Sleep(50 * time.Millisecond) // 心跳间隔通常为 50ms 或更小
		rf.mu.Lock()
		if rf.state != "Leader" {
			rf.mu.Unlock()
			continue
		}
		rf.sendHeartbeats()
		rf.mu.Unlock()
	}
}

func (rf *Raft) sendHeartbeats() {
	for i := range rf.peers {
		if i == rf.me {
			continue
		}

		go func(server int) {
			rf.mu.Lock()
			prevLogIndex := rf.nextIndex[server] - 1
			prevLogTerm := 0
			if prevLogIndex >= 0 {
				prevLogTerm = rf.log[prevLogIndex].Term
			}

			// 如果有未复制的日志条目，发送它们
			entries := []LogEntry{}
			if rf.nextIndex[server] < len(rf.log) {
				entries = rf.log[rf.nextIndex[server]:]
			}

			args := AppendEntriesArgs{
				Term:         rf.currentTerm,
				LeaderId:     rf.me,
				PrevLogIndex: prevLogIndex,
				PrevLogTerm:  prevLogTerm,
				Entries:      entries, // 这里会发送实际的日志条目
				LeaderCommit: rf.commitIndex,
			}
			rf.mu.Unlock()

			reply := AppendEntriesReply{}
			if rf.sendAppendEntries(server, args, &reply) {
				rf.mu.Lock()
				defer rf.mu.Unlock()

				if reply.Term > rf.currentTerm {
					// 如果发现更高的任期，转换为 Follower
					rf.currentTerm = reply.Term
					rf.state = "Follower"
					rf.votedFor = -1
					rf.persist()
				} else if reply.Success {
					// 如果成功，更新 nextIndex 和 matchIndex
					rf.matchIndex[server] = prevLogIndex + len(entries)
					rf.nextIndex[server] = rf.matchIndex[server] + 1
					rf.updateCommitIndex()
				} else {
					// 如果失败，尝试回退 nextIndex
					rf.nextIndex[server] = max(1, rf.nextIndex[server]-1)
				}
			}
		}(i)
	}
}

func (rf *Raft) updateCommitIndex() {
	for i := len(rf.log) - 1; i > rf.commitIndex; i-- {
		count := 1
		for j := range rf.peers {
			if rf.matchIndex[j] >= i {
				count++
			}
		}
		if count > len(rf.peers)/2 && rf.log[i].Term == rf.currentTerm {
			rf.commitIndex = i
			break
		}
	}
}

func (rf *Raft) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// 1. 如果 leader 的 term 小于当前节点的 term，直接拒绝
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	// 2. 如果 leader 的 term 更新，转换为 follower 并更新 term
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.state = "Follower"
		rf.votedFor = -1
		rf.persist()
	}

	// 重置选举超时时间，因为收到了合法的 AppendEntries
	rf.lastHeard = time.Now()

	if args.PrevLogIndex < 0 || args.PrevLogIndex >= len(rf.log) || args.PrevLogTerm != rf.log[args.PrevLogIndex].Term {
		reply.Term = rf.currentTerm
		reply.Success = false

		return
	}
	if args.PrevLogIndex+1 < len(rf.log) {
		rf.log = rf.log[:args.PrevLogIndex+1]
		rf.persist()
	}

	// Append any new entries not already in the log
	rf.log = append(rf.log, args.Entries...)

	rf.persist()

	// If leaderCommit > commitIndex, set commitIndex =
	// min(leaderCommit, index of last new entry)
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, len(rf.log)-1)
	}

	reply.Term = rf.currentTerm
	reply.Success = true

}

func (rf *Raft) Apply() {
	for {
		time.Sleep(10 * time.Millisecond)
		rf.mu.Lock()
		for rf.lastApplied < rf.commitIndex {
			rf.lastApplied += 1
			msg := ApplyMsg{
				Index:       rf.lastApplied,
				Command:     rf.log[rf.lastApplied].Command,
				UseSnapshot: false,
				Snapshot:    nil,
			}
			rf.applyCh <- msg
		}
		rf.mu.Unlock()
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here.
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.state = "Follower"
	rf.lastHeard = time.Now()
	rf.applyCh = applyCh
	rf.log = []LogEntry{{Index: 0, Term: 0, Command: nil}}
	rf.electionTimeout = time.Duration(150+rand.Intn(150)) * time.Millisecond
	rf.commitIndex = 0
	rf.lastApplied = 0
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	go rf.runElectionTimer()
	go rf.runHeartbeatTimer()
	go rf.Apply()
	return rf
}
