package raft

type InstallSnapShotArgs struct {
	Term              int
	LeaderID          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
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

//RPC -> excuted by the receiver
func (rf *Raft) InstallSnapShot(args *InstallSnapShotArgs, reply *InstallSnapShotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		return
	}

	reply.Term = args.Term
	rf.changeToFollower(args.Term, -1)

	curLastSnapIndex := rf.getFirstIndex()
	curLastIndex := rf.getLastLogIndex()

	if curLastSnapIndex < args.LastIncludedIndex {
		// snapshot should be installed
		reply.Term = args.Term
		//install snapshot
		rf.commitQueue = append(rf.commitQueue, ApplyMsg{
			CommandValid:  false,
			SnapshotValid: true,
			Snapshot:      args.Data,
			SnapshotTerm:  args.LastIncludedTerm,
			SnapshotIndex: args.LastIncludedIndex,
		})

		if curLastIndex > args.LastIncludedIndex {
			//dispose old logs
			old_logs := rf.log
			rf.log = make([]LogEntry, curLastIndex-args.LastIncludedIndex+1)
			copy(rf.log, old_logs[args.LastIncludedIndex-curLastSnapIndex:])
			rf.log[0].Command = nil

			if args.LastIncludedIndex < rf.commitIndex {
				for i := args.LastIncludedIndex + 1; i <= rf.commitIndex; i++ {
					rf.commitQueue = append(rf.commitQueue, ApplyMsg{
						CommandValid: true,
						CommandIndex: i,
						Command:      rf.getCommandByIndex(i),
					})
				}
			} else {
				rf.commitIndex = args.LastIncludedIndex
			}
		} else {
			rf.log = make([]LogEntry, 0)
			rf.log = append(rf.log, LogEntry{
				Term:    args.LastIncludedTerm,
				Index:   args.LastIncludedIndex,
				Command: nil,
			})

			rf.commitIndex = args.LastIncludedIndex
		}

		rf.persister.SaveStateAndSnapshot(rf.serializeState(), args.Data)
		rf.cv.Broadcast()
	}

	rf.resetElection_Timeout()

}

func (rf *Raft) sendInstallSnapshot(peerId int, args *InstallSnapShotArgs, reply *InstallSnapShotReply) bool {
	ok := rf.peers[peerId].Call("Raft.InstallSnapShot", args, reply)
	return ok
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
