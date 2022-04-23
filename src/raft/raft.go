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
	//	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labrpc"
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

// raft states
type State int64
const (
	Leader State = iota
	Follower
	Candidate
)

func (s State) String() string {
	switch s {
	case Leader:
		return "LEADER"
	case Follower:
		return "FOLLOWER"
	case Candidate:
		return "CANDIDATE"
	}
	return "UNKNOWN"
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	state						State
	currentTerm 		int
	votesCount 			int32

	tickerResetFlag	bool // for timer purposes
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term 				int
	CandidateId int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term 					int
	VoteGranted 	bool
}

type AppendEntriesRequest struct {
	Term 				int
	RequesterId int
}

type AppendEntriesReply struct {
	Success 	bool
	ReplierId int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
		// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	var term int = rf.currentTerm
	var isLeader bool = rf.state == Leader

	return term, isLeader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}


//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
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


//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}


func (rf* Raft) AppendEntriesHandler(request *AppendEntriesRequest, reply *AppendEntriesReply) {
	reply.ReplierId = rf.me

	rf.mu.Lock()
	if (rf.state == Candidate) {
		RichDebug(dVote, rf.me, "AppendEntries received, converting back to follower state")
		rf.state = Follower
	}
	
	if (rf.currentTerm < request.Term) {
		RichDebug(dVote, rf.me, "Received message with higher term %d > %d. Converting to follower if not", request.Term, rf.currentTerm)
		rf.currentTerm = request.Term
		rf.state = Follower
	}

	reply.Success = true
	rf.mu.Unlock()
	rf.resetTicker()
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	RichDebug(dVote, rf.me, "New vote has been requested; my term = %d, request term = %d", rf.currentTerm, args.Term)

	if (rf.currentTerm < args.Term) {
		RichDebug(dVote, rf.me, "Voting for %d", args.CandidateId)
		rf.currentTerm = args.Term
		rf.state = Follower
		reply.VoteGranted = true
		reply.Term = rf.currentTerm
	}	
}

func (rf *Raft) startElectionTerm() bool {	
	majority := int32((len(rf.peers) / 2) + 1)
	rf.resetVotes()

	rf.mu.Lock()
	rf.state = Candidate
	rf.currentTerm = rf.currentTerm + 1
	requestVote := RequestVoteArgs { rf.currentTerm, rf.me }
	RichDebug(dTerm, rf.me, "Starting new election term (term: %d, majority: %d)", rf.currentTerm, majority)
	rf.mu.Unlock()

	rf.addVote() // vote for myself
	rf.resetTicker()

	for i := 0; i < len(rf.peers); i++ {		
		if i == rf.me {
			continue // skip candidate peer
		}

		go func(peerId int) {
			requestReply := RequestVoteReply { 0, false }
			RichDebug(dTerm, rf.me, "Request votes from server %d", peerId)
			success := rf.sendRequestVote(peerId, &requestVote, &requestReply)
			if !success {
				RichDebug(dTerm, rf.me, "No response from peer %d", peerId)
			} else {
				RichDebug(dTerm, rf.me, "Vote response from peer %d received (peer term: %d)", peerId, requestReply.Term)
				rf.mu.Lock()
				if requestReply.Term == rf.currentTerm && requestReply.VoteGranted && rf.state == Candidate {
					RichDebug(dTerm, rf.me, "Vote granted from peer %d", peerId)
					
					if rf.addVote() >= majority {
						RichDebug(dTerm, rf.me, "Peer has been elected as a new leader by majority (%d); term = %d", majority, rf.currentTerm)
						rf.state = Leader
						go rf.startSendingHeartbeatsToFollowers()	
					}
				}
				rf.mu.Unlock()
			}
		}(i) // run anonymous function with correct id
	}
	return true
}

func (rf *Raft) startSendingHeartbeatsToFollowers() {
	RichDebug(dLeader, rf.me, "Starting to send heartbeats to followers")
	for rf.killed() == false {
		_, isLeader := rf.GetState()
		if !isLeader {
			RichDebug(dLeader, rf.me, "Not a leader, stopping to send heartbeats to followers")
			break
		}

		rf.mu.Lock()
		heartbeat := AppendEntriesRequest { rf.currentTerm, rf.me } 
		rf.mu.Unlock()

		for i:= 0; i < len(rf.peers); i ++ {
			if i == rf.me {
				continue
			}
	
			heartbeatReply := AppendEntriesReply { false, -1 }
	
			go func(peerId int) {
				rf.sendAppendEntry(peerId, &heartbeat, &heartbeatReply)
				// TODO what to do when heartbeat success is false? 
			}(i)
		}
		time.Sleep(200 * time.Millisecond)
	}
}

func (rf *Raft) addVote() int32 { // return current numbers of votes
	atomic.AddInt32(&rf.votesCount, 1)
	return atomic.LoadInt32(&rf.votesCount)
}

func(rf *Raft) resetVotes() int32 {
	atomic.StoreInt32(&rf.votesCount, 0)
	return atomic.LoadInt32(&rf.votesCount)
}

func (rf *Raft) resetTicker() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.tickerResetFlag = true
}

//
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
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf* Raft) sendAppendEntry(server int, request *AppendEntriesRequest, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntriesHandler", request, reply)
	return ok
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).


	return index, term, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
	for rf.killed() == false {	
		startTime := time.Now().UnixMilli()
		electionTimeout := int64(300 + rand.Intn(600)) // election timeout should be from 150ms to 300ms
	
		for { // if reset timer is false, continue
			time.Sleep(15 * time.Millisecond)

			_, isLeader := rf.GetState()
			if isLeader {
				break
			}

			now := time.Now().UnixMilli()

			if now - startTime > electionTimeout {
				RichDebug(dTimer, rf.me, "Election timeout")
				
				// switch to candidate state & request vote	
				go rf.startElectionTerm()
			}

			rf.mu.Lock()
			if rf.tickerResetFlag {
				rf.tickerResetFlag = false
				rf.mu.Unlock()
				break
			}
			rf.mu.Unlock()
		}
	}
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.mu.Lock()
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.state = Follower
	rf.currentTerm = 0
	rf.votesCount = 0
	rf.mu.Unlock()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	RichDebug(dLog, rf.me, "Starting")
	go rf.ticker()

	return rf
}
