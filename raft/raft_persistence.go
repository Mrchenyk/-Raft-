package raft

import (
	"bytes"
	"course/labgob"
)

//Raft节点持久化，主要是Raft的序列化和反序列化
func (rf *Raft) persist() {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.votedFor)
	e.Encode(rf.currentTerm)
	rf.log.persist(e)
	raftstate := w.Bytes()
	rf.persister.Save(raftstate, rf.log.snapshot)
}


func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}

	var currentTerm int
	var votedFor int

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	if err:=d.Decode(&votedFor);err!=nil{
		LOG(rf.me,rf.currentTerm,DTerm,"readPersist:Decode votedFor error")
	}
	rf.votedFor = votedFor
	
	if err:=d.Decode(&currentTerm);err!=nil{
		LOG(rf.me,rf.currentTerm,DTerm,"readPersist:Decode currentTerm error")
	}
	rf.currentTerm = currentTerm


	if err:=rf.log.readPersist(d);err!=nil{
		LOG(rf.me,rf.currentTerm,DTerm,"readPersist:Decode log error")
	}

	rf.log.snapshot = rf.persister.ReadSnapshot()

	if rf.log.snapLastIndex>rf.commitIndex{
		rf.commitIndex=rf.log.snapLastIndex
		rf.lastApplied=rf.log.snapLastIndex
	}

	LOG(rf.me,rf.currentTerm,DTerm,"readPersist:currentTerm:%d,votedFor:%d,log:%v",rf.currentTerm,rf.votedFor,rf.log)
}