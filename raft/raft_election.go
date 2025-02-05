package raft

import (
	"math/rand"
	"time"
)

func (rf *Raft) resetElectionTImeout() {
	rf.electionStart = time.Now()
	randRange := int64(electionTimeoutMax - electionTimeoutMin)
	rf.electionTimeout = electionTimeoutMin + time.Duration(rand.Int63()%randRange)
}

func (rf *Raft) isElectionTimeout() bool {
	return time.Since(rf.electionStart) > rf.electionTimeout
}

//比较哪一个日志更新
func (rf *Raft) isMoreUpToDate(candidateIndex, candidateTerm int) bool{

	lastIndex,lastTerm:=rf.log.last()
	if lastTerm!=candidateTerm{
		return lastTerm>candidateTerm
	}
	return lastIndex>candidateIndex
}

//RPC
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {

	Term int
	CandidateID int

	LastLogIndex int
	LastLogTerm int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	//都要大写开头不然RPC框架无法序列化和反序列化
	Term int 
	VoteGranted bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term=rf.currentTerm
	reply.VoteGranted=false

	//align term
	if args.Term<rf.currentTerm{
		return
	}

	if args.Term>rf.currentTerm{
		rf.becomeFollower(args.Term)
	}

	//check for votefor
	if rf.votedFor!=-1{
		return
	}

	//检查候选者是否up to date
	if rf.isMoreUpToDate(args.LastLogIndex,args.LastLogTerm){
		LOG(rf.me,rf.currentTerm,DVote,"Peer:%v reject vote from %v,candidate's log is not up to date",rf.me,args.CandidateID)
		return 
	}

	reply.VoteGranted=true
	rf.votedFor=args.CandidateID
	rf.persist()
	rf.resetElectionTImeout()
}

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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}



func (rf *Raft) electionTicker() {
	
	for !rf.killed() {
		// Check if a leader election should be started.
		rf.mu.Lock()
		if rf.role!=Leader&& rf.isElectionTimeout(){
			rf.becomeCandidate()
			LOG(rf.me,rf.currentTerm,DInfo,"Peer:%v start election",rf.me)
			go rf.startElection(rf.currentTerm)
		}
		rf.mu.Unlock()

		// pause for a random amount of time between 50 and 350
		// milliseconds.
		ms := 50 + (rand.Int63() % 300)
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

func (rf *Raft)startElection(term int){
	votes:=0
	askVoteFromPeer:=func(peer int,args *RequestVoteArgs){
		reply:=&RequestVoteReply{}
		ok:=rf.sendRequestVote(peer,args,reply)

		rf.mu.Lock()
		defer rf.mu.Unlock()
		if !ok{
			LOG(rf.me,rf.currentTerm,DDebug,"Ask for vote from %d failed",peer)
			return 
		}

		if reply.Term>rf.currentTerm{
			rf.becomeFollower(reply.Term);
			return
		}

		if rf.contextLostLocked(Candidate,term){
			LOG(rf.me,rf.currentTerm,DVote,"Lost context,abort RequestVote from %d",peer)
			return
		}

		if reply.VoteGranted {
			votes++
			if(votes>len(rf.peers)/2){
				rf.becomeLeader()
				go rf.replicationTicker(term)
			}
		}

	}

	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.contextLostLocked(Candidate,term){
		LOG(rf.me,rf.currentTerm,DVote,"Lost Candidate to %s,abort RequestVote",rf.role)
		return
	}

	lastIdx,lastTerm:=rf.log.last()

	for peer:=0;peer<len(rf.peers);peer++{
		if peer==rf.me{
			votes++;
			continue;
		}
		
		args:=&RequestVoteArgs{
			Term: rf.currentTerm,
			CandidateID: rf.me,
			LastLogIndex: lastIdx,
			LastLogTerm: lastTerm,
		}

		go askVoteFromPeer(peer,args)
	}
}