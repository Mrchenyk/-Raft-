package raft

import (
	"course/labrpc"
	"sync"
	"sync/atomic"
	"time"

)

const(
	electionTimeoutMin time.Duration=250*time.Millisecond
	electionTimeoutMax time.Duration=400*time.Millisecond
	replicateInterval time.Duration=200*time.Millisecond
)

const(
	InvalidIndex int=0
	InvalidTerm int=0
)


type Role string
const(
	Follower Role = "Follower"
	Candidate Role = "Candidate"
	Leader Role="Leader"
)


type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

// Raft节点定义
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	role Role
	currentTerm int 
	votedFor int

	electionStart time.Time
	electionTimeout time.Duration

	//每个节点的日志
	log []LogEntry

	//Leader节点使用
	//节点的视图
	nextIndex []int
	matchIndex []int

	//apply loop 的字段
	applyCh chan ApplyMsg
	commitIndex int
	lastApplied int
	applyCond *sync.Cond
}

func (rf *Raft)becomeFollower(term int){
	if term<rf.currentTerm {
		LOG(rf.me,rf.currentTerm,DError,"can't become Follower,lower term:%d",term)
		return
	}
	LOG(rf.me,rf.currentTerm,DLog,"%s->Follower,For T:%d->T:%d",rf.role,rf.currentTerm,term)

	shouldPersistence:=term!=rf.currentTerm	
	if term>rf.currentTerm{
		rf.votedFor=-1
	}

	rf.role=Follower
	rf.currentTerm=term

	if(shouldPersistence){
		rf.persist()
	}
}

func (rf *Raft)becomeCandidate(){
	if rf.role==Leader{
		LOG(rf.me,rf.currentTerm,DError,"leader can't become Candidate,node:%v",rf.me)
		return 
	}
	LOG(rf.me,rf.currentTerm,DVote,"%s->candidate,For T%d",rf.role,rf.currentTerm+1)
	rf.currentTerm++
	rf.role=Candidate
	rf.votedFor=rf.me 

	rf.persist()
}

func (rf *Raft)becomeLeader(){
	if rf.role!=Candidate{
		LOG(rf.me,rf.currentTerm,DError,"only candidate can become leader")
		return
	}

	LOG(rf.me,rf.currentTerm,DLeader,"become leader in T:%v",rf.currentTerm)
	rf.role=Leader

	//初始化每个节点的nextIndex和matchIndex
	for peer:=0;peer<len(rf.peers);peer++{
		rf.nextIndex[peer]=len(rf.log)
		rf.matchIndex[peer]=0
	}
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	return rf.currentTerm, rf.role==Leader
}



// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (PartD).

}



// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}


func (rf *Raft)contextLostLocked(role Role,term int)bool{
	return !(rf.currentTerm==term&&rf.role==role);

}

func (rf *Raft)firstLogFor(term int)int{
	for index,entry:=range rf.log{
		if entry.Term==term{
			return index
		}else if(entry.Term>term){
			break
		}
	}
	return InvalidIndex
}



// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	rf.role=Follower
	rf.currentTerm=1
	rf.votedFor=-1

	//放入空节点避免边界判断
	rf.log = append(rf.log, LogEntry{Term:InvalidTerm})
	rf.matchIndex=make([]int, len(rf.peers))
	rf.nextIndex=make([]int, len(rf.peers))

	//用于日志apply的初始化
	rf.applyCh=applyCh
	rf.applyCond=sync.NewCond(&rf.mu)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	LOG(rf.me,rf.currentTerm,DInfo,"make Peer:%v success!",rf.role)
	// 全局Ticker
	go rf.electionTicker()
	go rf.applyTicker()
	return rf
}