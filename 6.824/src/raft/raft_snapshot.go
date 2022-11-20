package raft

type InstallSnapShotArgs struct {
	Term              int
	LeaderID          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Offset            int
	Data              []byte

	Done bool
}

type InstallSnapShotReply struct {
	Term int
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
	rf.mu.Lock()
	defer rf.mu.Unlock()

	curFirstIndex := rf.getFirstIndex()
	curLastIndex := rf.getLastLogIndex()
	//illegal snapshot call
	if curFirstIndex >= index {
		DPrintf("Snapshot Failed - S%v, T%v, Illegal Snapshot call", rf.me, rf.currentTerm)
		return
	}

	old_log := rf.log
	rf.log = make([]LogEntry, curLastIndex-index+1)
	copy(rf.log, old_log[index-curFirstIndex:])
	rf.log[0].Command = nil //set the first log as dummy log

	state := rf.serializeState()

	rf.persister.SaveStateAndSnapshot(state, snapshot)

}
