package raft

import (
	"time"
)

const HEARTBEAT_INTEVAL = 100

func (rf *Raft) appendentry_ticker() {
	for !rf.killed() {
		time.Sleep(HEARTBEAT_INTEVAL * time.Millisecond)
		rf.mu.Lock()
		if rf.role != LEADER {
			rf.mu.Unlock()
			continue
		}
		//ensured to be in the LEADER state
		rf.HeartBeatAll()
		rf.mu.Unlock()
	}
}

//only called by LEADER
func (rf *Raft) HeartBeatAll() {
	DPrintf("T[%v] - S[%v] - R[%v] Sent HeartBeat", rf.currentTerm, rf.me, rf.role)
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

	// is there any new log to be appended to follower
	// if rf.getLastLogIndex() >= rf.nextIndex[server] {
	// logs := make([]LogEntry, rf.getLastLogIndex()+1-rf.nextIndex[server])
	logs := make([]LogEntry, rf.getLastLogIndex()-rf.nextIndex[server]+1)
	copy(logs, rf.log[rf.nextIndex[server]-rf.getFirstIndex():])
	// copy(logs, rf.log[rf.nextIndex[server]:])
	// logs := rf.log[rf.nextIndex[server]:]
	//add log info to args
	args.Entries = logs

	//for consistency check (leader's log[PrevLogIndex] == follower's log[PrevLogIndex])
	args.PrevLogIndex = rf.nextIndex[server] - 1 //
	args.PrevLogTerm = rf.getTermByIndex(args.PrevLogIndex)
	args.LeaderID = rf.me
	args.Term = rf.currentTerm
	args.LeaderCommit = rf.commitIndex

	ok := rf.sendAppendEntries(server, &args, &repl)

	if ok {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		//current leader is out of date (for some reason)
		if args.Term < repl.Term {
			rf.changeToFollower(repl.Term, -1)
			return
		}

		if rf.role != LEADER {
			return
		}

		if repl.Term < rf.currentTerm || args.Term != rf.currentTerm {
			return
		}

		if args.PrevLogIndex != rf.nextIndex[server]-1 {
			return
		}

		//consistency check failed -> get the index of last non-conflicting log -> resend
		if !repl.Success {
			//update nextIndex[server], ensure next RPC can work.
			rf.nextIndex[server] = repl.NextIndex
			// ok = rf.sendAppendEntries(server, &args, &repl)
			return
		}

		// rpc reply success -> update leader's nextIndex[server] to the next new entry
		if repl.Success {
			rf.nextIndex[server] = repl.NextIndex
			rf.matchIndex[server] = args.PrevLogIndex + len(logs)

			//update commitIndex in leader
			matched_map := make(map[int]int)
			for i := range rf.peers {
				if i == rf.me {
					continue
				}
				matched_map[rf.matchIndex[i]] += 1
			}

			ok_idx := rf.commitIndex
			old_commitIndex := rf.commitIndex
			for k, v := range matched_map {
				if v+1 > (len(rf.peers)-1)/2 {
					if k > ok_idx {
						ok_idx = k
					}
				}
			}
			if rf.getTermByIndex(ok_idx) == rf.currentTerm {
				rf.commitIndex = ok_idx
				DPrintf("COMMITIDX UPDATE : T[%v] - S[%v] - R[%v] CommitIndex Update to %v", rf.currentTerm, rf.me, rf.role, rf.commitIndex)
			}

			if rf.commitIndex > old_commitIndex {
				for i := old_commitIndex + 1; i <= rf.commitIndex; i++ {
					rf.commitQueue = append(rf.commitQueue, ApplyMsg{
						CommandValid: true,
						Command:      rf.log[i].Command,
						CommandIndex: i,
					})
				}
				rf.cv.Broadcast()
			}
		}

	}

}
