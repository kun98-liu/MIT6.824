package raft

type AppendEntriesArgs struct {
	Term     int
	LeaderID int

	Entries []LogEntry

	PrevLogIndex int
	PrevLogTerm  int

	LeaderCommit int
}

type AppendEntriesReply struct {
	Term      int
	Success   bool
	NextIndex int
}

//RPC func
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("T[%v] - S[%v] - R[%v] 's AppendEntries RPC is called, args : %v", rf.currentTerm, rf.me, rf.role, args)
	if args.Term < rf.currentTerm { //reject all out-of-date rpc
		reply.Success = false
		reply.Term = rf.currentTerm
		return
	}
	rf.resetElection_Timeout()

	if args.Term > rf.currentTerm {
		rf.changeToFollower(args.Term, -1)
	}
	//do consistency check
	reply.Term = args.Term

	cur_LastIndex := rf.getLastLogIndex()
	if args.PrevLogIndex > cur_LastIndex {
		reply.Success = false
		reply.NextIndex = cur_LastIndex + 1
		return
	}
	//consistenct check
	if args.PrevLogIndex == cur_LastIndex && args.PrevLogTerm == rf.getLastLogTerm() {

		DPrintf("CHECK_ACK : T[%v] - S[%v] - [%v]: consistent check OK", rf.currentTerm, rf.me, rf.role)
		append_num := 0
		if len(args.Entries) > 0 {
			for _, entry := range args.Entries {
				if entry.Index <= cur_LastIndex {
					continue
				}
				rf.log = append(rf.log, entry)
				append_num++
				DPrintf("APPEND_ACK : T[%v] - S[%v] - R[%v]: FOLLOWER -> NEW Log appended, %v", rf.currentTerm, rf.me, rf.role, entry.Index)
			}
		}
		reply.Success = true
		reply.NextIndex = cur_LastIndex + append_num + 1

		if args.LeaderCommit > rf.commitIndex {
			old_commitIndex := rf.commitIndex
			if args.LeaderCommit < rf.getLastLogIndex() {
				rf.commitIndex = args.LeaderCommit
			} else {
				rf.commitIndex = rf.getLastLogIndex()
			}
			DPrintf("COMMITIDX UPDATE : T[%v] - S[%v] - R[%v] : CommitIndex Update to %v", rf.currentTerm, rf.me, rf.role, rf.commitIndex)
			for i := old_commitIndex + 1; i <= rf.commitIndex; i++ {
				rf.commitQueue = append(rf.commitQueue, ApplyMsg{
					CommandValid: true,
					Command:      rf.getCommandByIndex(i),
					CommandIndex: i,
				})

				rf.cv.Broadcast()
			}
		}

	} else {
		DPrintf("CHECK_FAIL : T[%v] - S[%v]- R[%v]: consistent check Fail", rf.currentTerm, rf.me, rf.role)
		lastTerm := rf.getLastLogTerm() - 1
		idx := cur_LastIndex
		for idx > 0 && rf.getTermByIndex(idx) > lastTerm {
			idx--
		}
		rf.log = rf.log[:idx-rf.getFirstIndex()+1] //delete inconsistent logs of this term
		reply.NextIndex = idx + 1 + rf.getFirstIndex()
		reply.Success = false
	}

}

func (rf *Raft) sendAppendEntries(peerId int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[peerId].Call("Raft.AppendEntries", args, reply)
	return ok
}
