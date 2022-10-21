package raft

import (
	"math/rand"
	"time"
)

//initial election timeout, will be overwritten after the first Term
const INTIAl_ELECTION_TIMEOUT = 300

const RANDOM_ELECTION_TIMEOUT_MIN = 250
const RANDOM_ELECTION_TIMEOUT_MAX = 400

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) election_timeout_ticker() {
	for !rf.killed() {
		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().

		//check if a new election should be started by a const inteval -> 10ms
		time.Sleep(10 * time.Millisecond)
		rf.mu.Lock()
		if time.Now().After(rf.election_timeout_time) && (rf.role != LEADER) {
			DPrintf("START ELEC : S[%v]  ElECTION TIMEOUT -> Start election", rf.me)
			//start election
			rf.changeToCandidate()     //term incre
			rf.resetElection_Timeout() //new election timeout
			rf.StartElection()

		}
		rf.mu.Unlock()

	}
}

//called after lock
func (rf *Raft) resetElection_Timeout() {
	ELECTION_TIMEOUT := rand.Intn(RANDOM_ELECTION_TIMEOUT_MAX-RANDOM_ELECTION_TIMEOUT_MIN) + RANDOM_ELECTION_TIMEOUT_MIN
	rf.election_timeout_time = time.Now().Add(time.Duration(ELECTION_TIMEOUT) * time.Millisecond)
}

func (rf *Raft) StartElection() {

	//send requestVote RPC to all peers

	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go rf.CallForVote(i)
	}
}

func (rf *Raft) CallForVote(idx int) {
	args := RequestVoteArgs{}
	reply := RequestVoteReply{}

	args.CandiateID = rf.me
	args.Term = rf.currentTerm
	args.LastLogIndex = rf.getLastLogIndex()
	args.LastLogTerm = rf.getLastLogTerm()
	// DPrintf("Term[%v] - Server[%v,%v]: Sent RequesetVote -> Server[%v]", rf.currentTerm, rf.me, rf.role, idx)
	ok := rf.sendRequestVote(idx, &args, &reply)

	if ok {
		rf.mu.Lock()
		defer rf.mu.Unlock()

		//throw the reply, because it has been outdated
		if reply.Term < rf.currentTerm || args.Term != reply.Term {
			return
		}
		//
		if reply.Term > rf.currentTerm {
			rf.changeToFollower(reply.Term, -1) //set rf back to follower
			return
		}
		if reply.VoteGranted {
			rf.votedNum++
		}
		if rf.votedNum > len(rf.peers)/2 && rf.role == CANDIDATE {
			DPrintf("NEW LEADER : T[%v] - S[%v] - R[%v]: NEW LEADER ELECTED!!!", rf.currentTerm, rf.me, rf.role)
			rf.changeToLeader()
			time.Sleep(10 * time.Millisecond)
			rf.HeartBeatAll()
		}
	}

}
