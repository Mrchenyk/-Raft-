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
	if args.Term > rf.currentTerm {
		rf.becomeFollower(args.Term)
	}

	if(args.PrevLogIndex>=len(rf.log)){
		LOG(rf.me, rf.currentTerm, DLog2, "<-%d,Follower too short,reject log", args.LeaderId)
		return
	}
	if rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		LOG(rf.me, rf.currentTerm, DLog2, "<-%d,Reject log,prev index do not match", args.LeaderId)
		return
	}

	// 没有问题 把日志append到本地
	rf.log = append(rf.log[:args.PrevLogIndex+1], args.Entries...)

	reply.Success = true


	if args.LeaderCommit>rf.commitIndex{
		rf.commitIndex=args.LeaderCommit
		rf.applyCond.Signal()
	}
	rf.resetElectionTImeout()
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft)getMajorityMatched() int{
	temIndexes:=make([]int,len(rf.matchIndex))
	copy(temIndexes,rf.matchIndex)
	sort.Ints(sort.IntSlice(temIndexes))
	majorityIdx:=((len(rf.peers))-1)/2
	return temIndexes[majorityIdx]
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
			//回退一个term
			idx:=args.PrevLogIndex
			term:=args.PrevLogTerm
			for idx>0&&rf.log[idx].Term==term{
				idx--
			}
			rf.nextIndex[peer]=idx+1
			return
		}

		//更新nextIndex和matchIndex
		rf.matchIndex[peer]=args.PrevLogIndex+len(args.Entries)
		rf.nextIndex[peer]=rf.matchIndex[peer]+1

		majorityMatched:=rf.getMajorityMatched()
		if majorityMatched>rf.commitIndex{
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
			rf.matchIndex[peer]=len(rf.log)-1
			rf.nextIndex[peer]=len(rf.log)
			continue
		}


		preIdx:=rf.nextIndex[peer]-1
		preTerm:=rf.log[preIdx].Term
		args := &AppendEntriesArgs{
			Term:     rf.currentTerm,
			LeaderId: rf.me,
			PrevLogIndex: preIdx,
			PrevLogTerm: preTerm,
			Entries: rf.log[preIdx+1:],
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