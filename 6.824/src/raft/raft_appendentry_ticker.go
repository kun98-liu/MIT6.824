package raft

import (
	"time"
)

const HEARTBEAT_INTEVAL = 100
const APPEND_TICKER_RESOLUTION = 5

func (rf *Raft) appendentry_ticker() {
	for !rf.killed() {
		time.Sleep(APPEND_TICKER_RESOLUTION * time.Millisecond)
		rf.mu.Lock()
		if rf.role != LEADER {
			rf.mu.Unlock()
			continue
		}

		for i := 0; i < len(rf.peers); i++ {
			if i == rf.me {
				continue
			}

			if !time.Now().After(rf.append_timeout_time[i]) {
				continue
			}

			//send append entry or snapshot

			if rf.nextIndex[i] <= rf.getFirstIndex() {
				//send snapshot
			} else {
				//send append rpc

				go rf.sendHeartBeat(i)
			}
			rf.ResetAppendTimer(i, false)
		}
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

	logs := make([]LogEntry, rf.getLastLogIndex()-rf.nextIndex[server]+1)
	copy(logs, rf.log[rf.nextIndex[server]-rf.getFirstIndex():])
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

		//consistency check failed
		if !repl.Success {
			if repl.ConflictTerm == -1 {
				rf.nextIndex[server] = repl.ConflictIndex
			} else {
				findIdx := -1

				for i := rf.getLastLogIndex() + 1; i > rf.getFirstIndex(); i-- {
					if rf.getTermByIndex(i-1) == repl.ConflictTerm {
						findIdx = i
						break
					}
				}

				if findIdx != -1 {
					rf.nextIndex[server] = findIdx
				} else {
					rf.nextIndex[server] = repl.ConflictIndex
				}
			}
		}

		// rpc reply success -> update leader's nextIndex[server] to the next new entry
		if repl.Success {
			rf.nextIndex[server] = args.PrevLogIndex + len(args.Entries) + 1
			rf.matchIndex[server] = rf.nextIndex[server] - 1

			DPrintf("S%d update nextIndex[%d] = %d", rf.me, server, rf.nextIndex[server])

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
				if v+1 > len(rf.peers)/2 {
					if k > ok_idx {
						ok_idx = k
					}
				}
			}
			if rf.getTermByIndex(ok_idx) == rf.currentTerm {
				if ok_idx > old_commitIndex {
					rf.commitIndex = ok_idx
					DPrintf("COMMITIDX UPDATE : T[%v] - S[%v] - R[%v] CommitIndex Update to %v", rf.currentTerm, rf.me, rf.role, rf.commitIndex)

				}
			}

			if rf.commitIndex > old_commitIndex {
				for i := old_commitIndex + 1; i <= rf.commitIndex; i++ {
					rf.commitQueue = append(rf.commitQueue, ApplyMsg{
						CommandValid: true,
						Command:      rf.getCommandByIndex(i),
						CommandIndex: i,
					})
				}
				rf.cv.Broadcast()
			}
		}

		if rf.nextIndex[server] != rf.getLastLogIndex()+1 {

			rf.ResetAppendTimer(server, true)
		}

	}

}

func (rf *Raft) ResetAppendTimer(server int, immediate bool) {
	t := time.Now()

	if !immediate {
		t = t.Add(HEARTBEAT_INTEVAL * time.Millisecond)
	}

	rf.append_timeout_time[server] = t

}
