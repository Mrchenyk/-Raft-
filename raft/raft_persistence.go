package raft

import (
	"bytes"
	"course/labgob"
)

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.votedFor)
	e.Encode(rf.currentTerm)
	e.Encode(rf.log)
	raftstate := w.Bytes()
	rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}

	var currentTerm int
	var votedFor int
	var log []LogEntry

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	
	if err:=d.Decode(&currentTerm);err!=nil{
		LOG(rf.me,rf.currentTerm,DTerm,"readPersist:Decode currentTerm error")
	}

	rf.currentTerm = currentTerm

	if err:=d.Decode(&votedFor);err!=nil{
		LOG(rf.me,rf.currentTerm,DTerm,"readPersist:Decode votedFor error")
	}
	rf.votedFor = votedFor

	if err:=d.Decode(&log);err!=nil{
		LOG(rf.me,rf.currentTerm,DTerm,"readPersist:Decode log error")
	}
	rf.log = log

	LOG(rf.me,rf.currentTerm,DTerm,"readPersist:currentTerm:%d,votedFor:%d,log:%v",rf.currentTerm,rf.votedFor,rf.log)
}