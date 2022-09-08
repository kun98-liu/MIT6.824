package raft

import (
	"time"
)

const HEARTBEAT_INTEVAL = 200 // 2ms

func (rf *Raft) appendentry_ticker() {
	for !rf.killed() {
		time.Sleep(HEARTBEAT_INTEVAL * time.Millisecond)
		rf.mu.Lock()
		if rf.role != LEADER {
			rf.mu.Unlock()
			continue
		}
		DPrintf("Term[%v] - Server[%v] : Sent HeartBeat", rf.currentTerm, rf.me)
		//ensured to be in the LEADER state
		rf.HeartBeatAll()
		rf.mu.Unlock()
	}
}

//only called by LEADER
func (rf *Raft) HeartBeatAll() {
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go rf.sendHeartBeat(i)
	}
}

func (rf *Raft) sendHeartBeat(server int) {
	args := AppendEntriesArgs{}
	repl := AppendEntriesReply{}

	args.LeaderID = rf.me
	args.Term = rf.currentTerm
	ok := rf.sendAppendEntries(server, &args, &repl)

	if ok {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		if args.Term < repl.Term {
			rf.changeToFollower(repl.Term, -1)
			rf.resetElection_Timeout()
		}

	}
}
