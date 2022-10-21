package raft

//all functions of changing role should be called after lock()

func (rf *Raft) changeToCandidate() {
	rf.role = CANDIDATE
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.votedNum = 1
}

func (rf *Raft) changeToFollower(term int, votedFor int) {
	rf.role = FOLLOWER

	rf.currentTerm = term
	rf.votedFor = votedFor
	rf.votedNum = 0
}

func (rf *Raft) changeToLeader() {
	rf.role = LEADER
	rf.votedFor = -1

	//reset nextIndex[]
	idx := rf.getLastLogIndex() + 1
	for i := range rf.peers {
		rf.nextIndex[i] = idx
		rf.matchIndex[i] = 0
	}
	rf.votedNum = 0

}
