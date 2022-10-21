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

//three states of a raft server
const (
	LEADER    = 0
	CANDIDATE = 1
	FOLLOWER  = 2
)

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	cv        *sync.Cond          //Cond for CommitQueue
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentTerm int
	votedFor    int
	votedNum    int //number of votes received from peers for deciding whether this raft becomes a leader

	role int // state: LEADER | CANDIDATE | FOLLOWER

	election_timeout_time time.Time

	log []LogEntry // log entries
	// append_timeout_time []time.Time

	//volatile on leader
	nextIndex  []int
	matchIndex []int

	//volatile on all servers
	commitIndex int
	lastApplied int

	commitQueue []ApplyMsg //messagequeue for committing logs asyncly
}

type LogEntry struct {
	Term    int
	Index   int
	Command interface{}
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	//send RPC to all peers and get their terms
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm
	isleader = rf.role == LEADER

	return term, isleader
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
	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.killed() || rf.role != LEADER {
		return -1, -1, false
	}

	rf.log = append(rf.log, LogEntry{rf.currentTerm, rf.getLastLogIndex() + 1, command})
	DPrintf("NEWLOGADD: T[%v] - S[%v] - R[%v]: START -> NEW Log added, %v", rf.currentTerm, rf.me, rf.role, rf.log)
	rf.persist()

	//new log comes -> send appendentries rpc directly
	rf.HeartBeatAll()
	return rf.getLastLogIndex(), rf.currentTerm, true
}

func (rf *Raft) getFirstIndex() int {
	return rf.log[0].Index
}

func (rf *Raft) getLastLogIndex() int {
	return rf.log[len(rf.log)-1].Index
}

func (rf *Raft) getLastLogTerm() int {
	return rf.log[len(rf.log)-1].Term
}

func (rf *Raft) getTermByIndex(idx int) int {
	return rf.log[idx-rf.getFirstIndex()].Term
}

func (rf *Raft) getCommandByIndex(idx int) interface{} {
	return rf.log[idx-rf.getFirstIndex()].Command
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
	// Your initialization code here (2A, 2B, 2C).
	rf := &Raft{
		peers:                 peers,
		persister:             persister,
		me:                    me,
		currentTerm:           0,
		votedFor:              -1,
		role:                  FOLLOWER,
		election_timeout_time: time.Now().Add(INTIAl_ELECTION_TIMEOUT * time.Millisecond),
		log:                   make([]LogEntry, 0),
		nextIndex:             make([]int, len(peers)),
		matchIndex:            make([]int, len(peers)),
		commitIndex:           0,
		lastApplied:           0,
		commitQueue:           make([]ApplyMsg, 0),
	}
	rf.cv = sync.NewCond(&rf.mu)
	// add a dummy log
	rf.log = append(rf.log, LogEntry{0, 0, nil})

	//initialize nextIndex and matchIndex
	for i := range rf.peers {
		rf.nextIndex[i] = 1
	}
	for i := range rf.peers {
		rf.matchIndex[i] = 0
	}

	// initialize from persisted state before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.election_timeout_ticker()
	//heartbeat + append entries (only for leader)
	go rf.appendentry_ticker()

	go rf.apply(applyCh)

	return rf
}

//get ApplyMsg from CommitQueue and commit it into applyCh
func (rf *Raft) apply(applyCh chan ApplyMsg) {
	for !rf.killed() {
		rf.mu.Lock()
		for len(rf.commitQueue) == 0 {
			rf.cv.Wait()
		}

		msgs := rf.commitQueue
		rf.commitQueue = make([]ApplyMsg, 0)
		rf.mu.Unlock()

		for _, msg := range msgs {
			if msg.CommandValid {
				DPrintf("APPLY_LOG: S[%d] Apply Commnd IDX%d CMD: %v", rf.me, msg.CommandIndex, msg.Command)
			} else if msg.SnapshotValid {
				DPrintf("APPLY_SNAPSHOT: S[%d] Apply Snapshot. LII: %d, LIT: %d, snapShot: %v", rf.me, msg.SnapshotIndex, msg.SnapshotTerm, msg.Snapshot)
			} else {
				DPrintf("ERROR: S[%d], unknown Command!", rf.me)
				continue
			}
			// this may block
			rf.lastApplied = msg.CommandIndex
			applyCh <- msg
		}
	}
}
