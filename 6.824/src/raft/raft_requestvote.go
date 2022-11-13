package raft

//2A, requset vote

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).

	Term         int
	CandiateID   int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).

	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("T[%v] - S[%v] - R[%v]'s RequesetVote is called, args:%v", rf.currentTerm, rf.me, rf.role, args)
	//out-of-date rpc -> reject

	curLastLogIndex := rf.getLastLogIndex()
	curLastLogTerm := rf.getLastLogTerm()

	if args.Term < rf.currentTerm {
		// out of date request
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
	} else if args.Term == rf.currentTerm {
		reply.Term = args.Term
		// Term equal, check votedFor and log
		if (rf.votedFor == -1 || rf.votedFor == args.CandiateID) && (args.LastLogTerm > curLastLogTerm || args.LastLogTerm == curLastLogTerm && args.LastLogIndex >= curLastLogIndex) {
			reply.VoteGranted = true
			rf.changeToFollower(args.Term, args.CandiateID)
			rf.resetElection_Timeout()
		} else {
			reply.VoteGranted = false
		}
	} else {
		// args.Term > rf.currentTerm
		// check log
		reply.Term = args.Term
		if args.LastLogTerm > curLastLogTerm || args.LastLogTerm == curLastLogTerm && args.LastLogIndex >= curLastLogIndex {
			reply.VoteGranted = true
			rf.changeToFollower(args.Term, args.CandiateID)
		} else {
			reply.VoteGranted = false
			rf.changeToFollower(args.Term, -1)
		}
		rf.resetElection_Timeout()
	}

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
