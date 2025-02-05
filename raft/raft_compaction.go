package raft

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.log.doSnapshot(index,snapshot)
	rf.persist()
}

//RPC
type InstallSnapshotArgs struct {

	Term int
	LeaderId int

	LastIncludeIndex int
	LastIncludeTerm int 

	Snapshot []byte
}

type InstallSnapshotReply struct {
	Term int 
}

//Follower回调函数
func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term=rf.currentTerm

	//对齐Term
	if args.Term < rf.currentTerm {
		LOG(rf.me, rf.currentTerm, DSnap, "Reject Snap,Higher Term:T%d > T%d", rf.currentTerm,args.Term)
		return
	}

	if args.Term >= rf.currentTerm {
		rf.becomeFollower(args.Term)

	}

	if rf.log.snapLastIndex>=args.LastIncludeIndex{
		LOG(rf.me, rf.currentTerm, DSnap, "Reject Snap,Already Installed")
		return
	}

	rf.log.installSnapshot(args.LastIncludeIndex,args.LastIncludeTerm,args.Snapshot)
	rf.persist()
	rf.snapPending=true
	rf.applyCond.Signal()
}

//Leader发送RPC
func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

func (rf *Raft)installToPeer(peer ,term int,args *InstallSnapshotArgs){
	reply := &InstallSnapshotReply{}
	//发送日志同步RPC
	ok := rf.sendInstallSnapshot(peer, args, reply)

	rf.mu.Lock()
	defer rf.mu.Unlock()
	if !ok {
		LOG(rf.me, rf.currentTerm, DDebug, "Lost or crash: %d", peer)
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

	//更新match和next
	if(args.LastIncludeIndex>rf.matchIndex[peer]){
		rf.matchIndex[peer]=args.LastIncludeIndex
		rf.nextIndex[peer]=rf.matchIndex[peer]+1
	}
		
}