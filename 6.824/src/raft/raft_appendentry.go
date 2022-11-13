package raft

type AppendEntriesArgs struct {
	Term     int
	LeaderID int

	Entries []LogEntry

	PrevLogTerm  int
	PrevLogIndex int

	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool

	ConflictIndex int
	ConflictTerm  int
}

//RPC func
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// DPrintf("T[%v] - S[%v] - R[%v] 's AppendEntries RPC is called, args : %v", rf.currentTerm, rf.me, rf.role, args)
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

	cur_LastIndex := rf.getLastLogIndex()
	cur_FirstIndex := rf.getFirstIndex()
	reply.Term = args.Term

	if args.PrevLogIndex < cur_FirstIndex { // should not happen
		DPrintf("T[%v] - S[%v]- R[%v]: prevlogindex is in the snapshot", rf.currentTerm, rf.me, rf.role)
		reply.Success = false
		return
	}

	//consistency check fail, optimised following guide
	if args.PrevLogIndex > cur_LastIndex || rf.getTermByIndex(args.PrevLogIndex) != args.PrevLogTerm {
		DPrintf("CHECK_FAIL : T[%v] - S[%v]- R[%v]: consistent check Fail", rf.currentTerm, rf.me, rf.role)
		reply.Success = false

		if args.PrevLogIndex > cur_LastIndex {
			reply.ConflictIndex = cur_LastIndex + 1
			reply.ConflictTerm = -1
		} else {
			conflictTerm := rf.getTermByIndex(args.PrevLogIndex)
			reply.ConflictTerm = conflictTerm
			find_idx := args.PrevLogIndex

			for i := args.PrevLogIndex; i > 1; i-- {
				if rf.getTermByIndex(i-1) != conflictTerm {
					find_idx = i
					break
				}
			}
			reply.ConflictIndex = find_idx

		}
		return
	}

	//consistenct check success
	DPrintf("CHECK_ACK : T[%v] - S[%v] - [%v]: consistent check OK", rf.currentTerm, rf.me, rf.role)
	//check matched index
	matched_index := args.PrevLogIndex
	for i := 0; i < len(args.Entries); i++ {
		if matched_index+1 > cur_LastIndex {
			break
		}

		if rf.getTermByIndex(matched_index+1) != args.Entries[i].Term {
			break
		}

		matched_index++
	}

	if matched_index != args.PrevLogIndex+len(args.Entries) {
		//logs needed to be appended exist in Entries. -> delete logs after matched_index and append new ones
		rf.log = rf.log[0 : matched_index-rf.getFirstIndex()+1]
		rf.log = append(rf.log, args.Entries[matched_index-args.PrevLogIndex:]...)
		DPrintf("APPEND_ACK : T[%v] - S[%v] - R[%v]: FOLLOWER -> NEW Logs appended, INDEX: %v - %v",
			rf.currentTerm, rf.me, rf.role, args.Entries[matched_index-args.PrevLogIndex].Index, args.Entries[len(args.Entries)-1].Index)

		rf.persist()
	}

	reply.Success = true

	if args.LeaderCommit > rf.commitIndex {
		old_commitIndex := rf.commitIndex
		if args.LeaderCommit < rf.getLastLogIndex() {
			rf.commitIndex = args.LeaderCommit
		} else {
			rf.commitIndex = rf.getLastLogIndex()
		}

		if rf.commitIndex > old_commitIndex {

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
	}

}

func (rf *Raft) sendAppendEntries(peerId int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[peerId].Call("Raft.AppendEntries", args, reply)
	return ok
}
