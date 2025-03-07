package raft

import (
	"sort"
	"time"
)

type LogEntry struct {
	Term 		 int
	CommandValid bool
	Command      interface{}
	
}

type AppendEntriesArgs struct {
	Term     int
	LeaderId int
	
	//根据Index和Term唯一标识
	PrevLogIndex int
	PrevLogTerm int

	Entries []LogEntry

	//用于更新Follower的CommitIndex
	LeaderCommit int

}

type AppendEntriesReply struct {
	Term    int
	Success bool

	ConfilicIndex int
	ConfilicTerm int

}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term=rf.currentTerm
	reply.Success=false


	//对齐Term
	if args.Term < rf.currentTerm {
		LOG(rf.me, rf.currentTerm, DVote, "Lost leader to %s[T%d],abort replication", rf.role, rf.currentTerm)
		return
	}
	if args.Term >= rf.currentTerm {
		rf.becomeFollower(args.Term)
	}
	defer rf.resetElectionTImeout()

	if(args.PrevLogIndex>=rf.log.size()){
		reply.ConfilicIndex=rf.log.size()
		reply.ConfilicTerm=InvalidTerm
		LOG(rf.me, rf.currentTerm, DLog2, "<-%d,Follower too short,reject log", args.LeaderId)
		return
	}

	if rf.log.at(args.PrevLogIndex).Term != args.PrevLogTerm {
		reply.ConfilicTerm=rf.log.at(args.PrevLogIndex).Term
		reply.ConfilicIndex=rf.log.firstFor(reply.ConfilicTerm)
		LOG(rf.me, rf.currentTerm, DLog2, "<-%d,Reject log,prev index do not match", args.LeaderId)
		return
	}

	// 没有问题 把日志append到本地
	rf.log.appendFrom(args.PrevLogIndex,args.Entries)
	rf.persist()

	reply.Success = true


	if args.LeaderCommit>rf.commitIndex{
		rf.commitIndex=args.LeaderCommit
		rf.applyCond.Signal()
	}

}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) getMajorityIndex() int {
	tmpIndexes := make([]int, len(rf.peers))
	copy(tmpIndexes, rf.matchIndex)
	sort.Ints(sort.IntSlice(tmpIndexes))
	majorityIdx := (len(rf.peers) - 1) / 2
	LOG(rf.me, rf.currentTerm, DDebug, "Match index after sort: %v, majority[%d]=%d", tmpIndexes, majorityIdx, tmpIndexes[majorityIdx])
	return tmpIndexes[majorityIdx]
}

func (rf *Raft) startReplication(term int) bool {

	replicateToPeer := func(peer int, args *AppendEntriesArgs) {
		reply := &AppendEntriesReply{}
		//发送日志同步RPC
		ok := rf.sendAppendEntries(peer, args, reply)

		rf.mu.Lock()
		defer rf.mu.Unlock()
		if !ok {
			LOG(rf.me, rf.currentTerm, DDebug, "Ask for vote from %d failed", peer)
			return
		}

		//对齐term
		if reply.Term > rf.currentTerm {
			rf.becomeFollower(reply.Term)
			return
		}

		//检查上下文是否丢失
		if rf.contextLostLocked(Leader, term) {
			LOG(rf.me, rf.currentTerm, DVote, "Lost leader to %s[T%d],abort replication", term, rf.role, rf.currentTerm)
			return
		}

		//处理reply
		//如果index对不上就要往回找
		if !reply.Success {
			// 保存调整前的nextIndex值
			preIndex := rf.nextIndex[peer]
			// 如果回复的任期无效，说明Follower的日志比Leader短，则以Follower为准
			if reply.ConfilicTerm == InvalidTerm {
				rf.nextIndex[peer] = reply.ConfilicIndex
			} else {
				//否则回滚到冲突任期的第一个日志
				// 获取冲突任期的第一个日志索引
				firstIndex := rf.log.firstFor(reply.ConfilicTerm)
				// 如果找到了有效的日志索引，则将nextIndex设置为该索引值
				if firstIndex != InvalidIndex {
					rf.nextIndex[peer] = firstIndex
				} else {
					// 否则，以Follower为准
					rf.nextIndex[peer] = reply.ConfilicIndex
				}
			}

			// 确保nextIndex不会反复横跳，如果增加了则将其回退到调整前的值
			if rf.nextIndex[peer] > preIndex {
				rf.nextIndex[peer] = preIndex
			}
			return
		}

		//更新nextIndex和matchIndex
		rf.matchIndex[peer]=args.PrevLogIndex+len(args.Entries)
		rf.nextIndex[peer]=rf.matchIndex[peer]+1

		majorityMatched:=rf.getMajorityIndex()
		if majorityMatched>rf.commitIndex&&rf.log.at(majorityMatched).Term==rf.currentTerm{
			rf.commitIndex=majorityMatched
			rf.applyCond.Signal()
		}
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.contextLostLocked(Leader, term) {
		LOG(rf.me, rf.currentTerm, DVote, "Lost leader to %s[T%d],abort replication", term, rf.role, rf.currentTerm)
		return false
	}

	for peer := 0; peer < len(rf.peers); peer++ {
		if peer == rf.me {
			rf.matchIndex[peer]=rf.log.size()-1
			rf.nextIndex[peer]=rf.log.size()
			continue
		}


		preIdx:=rf.nextIndex[peer]-1
		if preIdx<rf.log.snapLastIndex{
			args:=&InstallSnapshotArgs{
				Term:rf.currentTerm,
				LeaderId: rf.me,
				LastIncludeIndex: rf.log.snapLastIndex,
				LastIncludeTerm: rf.log.snapLastTerm,
				Snapshot: rf.log.snapshot,
			}
			go rf.installToPeer(peer,rf.currentTerm,args)
			continue
		}

		preTerm:=rf.log.at(preIdx).Term
		args := &AppendEntriesArgs{
			Term:     rf.currentTerm,
			LeaderId: rf.me,
			PrevLogIndex: preIdx,
			PrevLogTerm: preTerm,
			Entries: rf.log.tail(preIdx+1),
			LeaderCommit: rf.commitIndex,
		}

		go replicateToPeer(peer, args)
	}
	return true
}

// 只有在任期内才能
func (rf *Raft) replicationTicker(term int) {
	for !rf.killed() {
		ok := rf.startReplication(term)
		if !ok {
			break
		}
		time.Sleep(replicateInterval)
	}
}