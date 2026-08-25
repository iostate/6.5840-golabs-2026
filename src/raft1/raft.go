package raft

// The file ../raftapi/raftapi.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// In addition,  Make() creates a new raft peer that implements the
// raft interface.

import (
	//	"bytes"
	"math/rand"
	"sync"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raftapi"
	tester "6.5840/tester1"
)

type Role int

const (
	Follower Role = iota // 0
	Candidate
	Leader
)

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *tester.Persister   // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentTerm int
	votedFor    int
	role        Role

	lastHeard         time.Time
	electionTimeout   time.Duration
	lastHeartbeatSent time.Time

	log []LogEntry

	commitIndex int
	lastApplied int
	nextIndex   []int
	matchIndex  []int
	applyCh     chan raftapi.ApplyMsg
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.role == Leader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (3C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

// how many bytes in Raft's persisted log?
func (rf *Raft) PersistBytes() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.persister.RaftStateSize()
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).

}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term        int
	CandidateId int
	// Note on checking most up to date log:

	// Same term? Most up-to-date log index
	// Last entries with different terms? Log with later term is more up-to-date.
	// For exercise 3A, we should be able to grant votes without using LastLogIndex / LastLogTerm
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	Term        int
	VoteGranted bool
}

type LogEntry struct {
	Term    int
	Command interface{}
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
	// Conflicting reply
	XTerm  int
	XIndex int
	XLen   int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// 1. Stale Leader
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	// 2. We have a newer term, step down to follower
	// adopt new term
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.role = Follower
	}

	rf.role = Follower
	rf.lastHeard = time.Now()

	reply.Term = rf.currentTerm

	// Checking this first so we don't go out of index
	if args.PrevLogIndex >= len(rf.log) {
		// Folower log is too short - no conflicting entry
		reply.XTerm = -1
		reply.XLen = len(rf.log)
		reply.Success = false
		return
	}

	if rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		// Conflict: report the term and where that term starts
		reply.XTerm = rf.log[args.PrevLogIndex].Term
		reply.XIndex = args.PrevLogIndex
		for reply.XIndex > 0 && rf.log[reply.XIndex-1].Term == reply.XTerm {
			reply.XIndex--
		}
		reply.XLen = len(rf.log)

		// reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	// Truncate conflicts
	// Figure 2, AppendEntries RPC steps 3 and 4
	// https://pdos.csail.mit.edu/6.824/papers/raft-extended.pdf
	for i, entry := range args.Entries {
		idx := args.PrevLogIndex + 1 + i // Where this leader entry belongs in my log
		if idx < len(rf.log) {
			// Existing entry conflicts with a new one
			// (same index, different terms)
			if rf.log[idx].Term != entry.Term {
				rf.log = rf.log[:idx]
				rf.log = append(rf.log, args.Entries[i:]...)
				break
			}
			continue
		}
		rf.log = append(rf.log, args.Entries[i:]...)
		break
	}

	// Figure 2: step 5 - if leaderCommit > commitIndex,
	// set commitIndex = min(leaderCommit, index of last new entry)
	lastNew := args.PrevLogIndex + len(args.Entries)
	if args.LeaderCommit > rf.commitIndex {
		// CommitIndex must never go backwards
		if args.LeaderCommit <= lastNew && args.LeaderCommit > rf.commitIndex {
			rf.commitIndex = args.LeaderCommit
		} else if lastNew < args.LeaderCommit && lastNew > rf.commitIndex {
			rf.commitIndex = lastNew
		}
	}
	reply.Success = true

}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// adopt new term
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.role = Follower
	}

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	myLastIndex := len(rf.log) - 1
	myLastTerm := rf.log[myLastIndex].Term
	upToDate := args.LastLogTerm > myLastTerm ||
		(args.LastLogTerm == myLastTerm && args.LastLogIndex >= myLastIndex)

	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && upToDate {
		rf.votedFor = args.CandidateId
		rf.lastHeard = time.Now()
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
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
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
	// Your code here (3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	currentTerm := rf.currentTerm
	isLeader := rf.role == Leader
	if !isLeader {
		return -1, currentTerm, false
	}

	rf.log = append(rf.log, LogEntry{
		Term:    currentTerm,
		Command: command,
	})
	index := len(rf.log) - 1 // dummy at 0, first command at 1
	rf.broadcastHeartbeats()

	return index, currentTerm, isLeader
}

func (rf *Raft) ticker() {
	for true {
		rf.mu.Lock()
		isLeader := rf.role == Leader
		if isLeader {
			rf.lastHeartbeatSent = time.Now()
			rf.broadcastHeartbeats()
		} else if time.Since(rf.lastHeard) >= rf.electionTimeout {
			rf.startElection()
		}
		rf.mu.Unlock()

		if isLeader {
			// Tester allows at most 10 heartbeats/sec.
			time.Sleep(100 * time.Millisecond)
		} else {
			time.Sleep(10 * time.Millisecond)
		}
	}
}

// We are the leader, and broadcasting heartbeats
// to followers
func (rf *Raft) broadcastHeartbeats() {

	term := rf.currentTerm

	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go func(server int) {
			for {
				rf.mu.Lock()
				if rf.role != Leader || rf.currentTerm != term {
					rf.mu.Unlock()
					return
				}
				if rf.nextIndex[server] < 1 {
					rf.nextIndex[server] = 1
				}
				if rf.nextIndex[server] > len(rf.log) {
					rf.nextIndex[server] = len(rf.log)
				}
				prev := rf.nextIndex[server] - 1
				args := AppendEntriesArgs{
					PrevLogIndex: prev,
					PrevLogTerm:  rf.log[prev].Term,
					LeaderCommit: rf.commitIndex,
					Term:         rf.currentTerm,
					LeaderId:     rf.me,
				}
				args.Entries = make([]LogEntry, len(rf.log[rf.nextIndex[server]:]))
				copy(args.Entries, rf.log[rf.nextIndex[server]:])
				rf.mu.Unlock()

				var reply AppendEntriesReply
				if ok := rf.sendAppendEntries(server, &args, &reply); !ok {
					return
				}

				rf.mu.Lock()
				if rf.role != Leader || rf.currentTerm != term {
					rf.mu.Unlock()
					return
				}

				if reply.Term > rf.currentTerm {
					rf.role = Follower
					rf.currentTerm = reply.Term
					rf.votedFor = -1
					rf.mu.Unlock()
					return
				}

				if reply.Success {
					n := args.PrevLogIndex + len(args.Entries)
					if n > rf.matchIndex[server] {
						rf.matchIndex[server] = n
						rf.nextIndex[server] = n + 1
					}
					rf.tryCommit()
					rf.mu.Unlock()
					return
				}

				oldNext := rf.nextIndex[server]
				rf.backupNextIndex(server, &reply)
				madeProgress := rf.nextIndex[server] < oldNext
				rf.mu.Unlock()
				if !madeProgress {
					return
				}
			}
		}(i)

	}
}

// Leader only. Caller must hold rf.mu.
func (rf *Raft) tryCommit() {
	for n := len(rf.log) - 1; n > rf.commitIndex; n-- {
		if rf.log[n].Term != rf.currentTerm {
			continue
		}
		count := 1 // this leader already has the entry
		for i := range rf.peers {
			if i != rf.me && rf.matchIndex[i] >= n {
				count++
			}
		}
		if count > len(rf.peers)/2 {
			rf.commitIndex = n
			return
		}
	}
}

// Leader only. Caller must hold rf.mu.
func (rf *Raft) backupNextIndex(server int, reply *AppendEntriesReply) {
	var next int
	if reply.XTerm == -1 {
		// Case 3: follower log too short
		next = reply.XLen
	} else {
		last := -1
		for i := len(rf.log) - 1; i >= 0; i-- {
			if rf.log[i].Term == reply.XTerm {
				last = i
				break
			}
		}
		if last == -1 {
			// Case 1: leader has no entry with XTerm
			next = reply.XIndex
		} else {
			// Case 2: leader has XTerm
			next = last + 1
		}
	}
	if next < 1 {
		next = 1
	}
	// Never increase nextIndex on a failure (stale RPC replies)
	if next < rf.nextIndex[server] {
		rf.nextIndex[server] = next
	}
}

func (rf *Raft) startElection() {

	rf.currentTerm++
	rf.role = Candidate
	rf.votedFor = rf.me
	rf.resetElectionTimer()

	votes := 1
	term := rf.currentTerm
	args := RequestVoteArgs{
		Term:         term,
		CandidateId:  rf.me,
		LastLogIndex: len(rf.log) - 1,
		LastLogTerm:  rf.log[len(rf.log)-1].Term,
	}

	// Contact each peer once w/ SendRequestVote
	for i := range rf.peers {
		if i == rf.me {
			continue
		}

		go func(server int) {
			var reply RequestVoteReply
			if ok := rf.sendRequestVote(server, &args, &reply); !ok {
				return
			}
			rf.mu.Lock()
			defer rf.mu.Unlock()

			if reply.Term > rf.currentTerm {
				rf.currentTerm = reply.Term
				rf.votedFor = -1
				rf.role = Follower
				rf.resetElectionTimer()
				return
			}
			if rf.role != Candidate || rf.currentTerm != term {
				return
			}

			if reply.VoteGranted {
				votes++

				// Checking if we got majority vote
				if votes > len(rf.peers)/2 {
					// Promote to leader, continue broadcasting heartbeats
					rf.role = Leader
					// Initialize all peer indexes
					for j := range rf.peers {
						rf.nextIndex[j] = len(rf.log)
						rf.matchIndex[j] = 0
					}
					rf.broadcastHeartbeats()
				}

			}
		}(i)
	}

}

func (rf *Raft) resetElectionTimer() {
	rf.lastHeard = time.Now()
	// 300-500ms
	ms := 300 + rand.Intn(200)
	rf.electionTimeout = time.Duration(ms) * time.Millisecond
}

// Watches commitIndex and when that number is ahead of lastApplied,
// sends the next committed commands to the tester
func (rf *Raft) applier() {
	for {
		time.Sleep(10 * time.Millisecond)
		rf.mu.Lock()
		if rf.commitIndex <= rf.lastApplied {
			rf.mu.Unlock()
			continue
		}

		msgs := make([]raftapi.ApplyMsg, 0, rf.commitIndex-rf.lastApplied)
		for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
			msgs = append(msgs, raftapi.ApplyMsg{
				CommandValid: true,
				Command:      rf.log[i].Command,
				CommandIndex: i,
			})
		}
		rf.lastApplied = rf.commitIndex
		rf.mu.Unlock()

		for _, msg := range msgs {
			rf.applyCh <- msg
		}
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
	persister *tester.Persister, applyCh chan raftapi.ApplyMsg) raftapi.Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (3A, 3B, 3C).
	// 3A
	rf.votedFor = -1
	rf.role = Follower
	rf.resetElectionTimer()

	// 3B
	// Initialize the dummy at index 0
	rf.log = []LogEntry{{Term: 0}}
	// Assign the channel to our parameter
	rf.applyCh = applyCh
	// Initialize slices to length of peers
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.applier()

	return rf
}
