package raft

type AppendEntriesArgs struct {
	Term     int
	LeaderID int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

//RPC func
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("Term[%v] - Server[%v,%v] 's AppendEntries RPC is called", rf.currentTerm, rf.me, rf.role)
	if args.Term >= rf.currentTerm {
		rf.changeToFollower(args.Term, -1)
		rf.resetElection_Timeout()
		reply.Success = true
	} else if args.Term < rf.currentTerm {
		reply.Success = false
		reply.Term = rf.currentTerm
	}

}

func (rf *Raft) sendAppendEntries(peerId int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[peerId].Call("Raft.AppendEntries", args, reply)
	return ok
}
