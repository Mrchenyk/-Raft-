package raft

import (
	"course/labgob"
	"fmt"
)

type RaftLog struct {
	snapLastIndex int
	snapLastTerm  int

	//[1,snapLastIndex]
	snapshot []byte
	//(snapLastIndex+1,lastIndex]
	taillog []LogEntry
}

func NewLog(snapLastIndex int, snapLastTerm int, snapshot []byte, entries []LogEntry) *RaftLog {
	rl := &RaftLog{
		snapLastIndex: snapLastIndex,
		snapLastTerm:  snapLastTerm,
		snapshot:      snapshot,
	}

	rl.taillog = append(rl.taillog, LogEntry{
		Term: snapLastTerm,
	})

	rl.taillog = append(rl.taillog, entries...)

	return rl
}

func (rl *RaftLog) last()(index,term int){
	i:=len(rl.taillog)-1
	return rl.snapLastIndex+i,rl.taillog[i].Term
}

//raft log序列化
func (rl *RaftLog) persist(w *labgob.LabEncoder) error {
	w.Encode(rl.snapLastIndex)
	w.Encode(rl.snapLastTerm)
	w.Encode(rl.taillog)
	return nil
}
//raft log反序列化
func (rl *RaftLog) readPersist(d *labgob.LabDecoder) error {
	var lastIdx int
	if err:= d.Decode(&lastIdx); err != nil {
		return fmt.Errorf("readPersist decode lastIdx error")
	}
	rl.snapLastIndex = lastIdx

	var lastTerm int
	if err:= d.Decode(&lastTerm); err != nil {
		return fmt.Errorf("readPersist decode lastTerm error")
	}
	rl.snapLastTerm = lastTerm

	var log []LogEntry
	if err:= d.Decode(&log); err != nil {
		return fmt.Errorf("readPersist decode tail log error")
	}
	rl.taillog = log

	return nil
}

//log大小
func (rl *RaftLog) size()int{
	return rl.snapLastIndex+len(rl.taillog)
}

//Index 转换
func (rl *RaftLog) idx(logicIndex int)int{
	if logicIndex<rl.snapLastIndex || logicIndex>rl.size(){
		panic(fmt.Sprintf("logicIndex %d out of range",logicIndex))
	}
	return logicIndex-rl.snapLastIndex
}

//根据下标访问
func (rl *RaftLog) at(logicIndex int)LogEntry{
	return rl.taillog[rl.idx(logicIndex)]
}

func (rl *RaftLog)firstFor(term int)int{
	for index,entry:=range rl.taillog{
		if entry.Term==term{
			return index+rl.snapLastIndex
		}else if(entry.Term>term){
			break
		}
	}
	return InvalidIndex
}

func (rl *RaftLog)doSnapshot(index int,snapshot []byte){
	idx:=rl.idx(index)

	rl.snapLastIndex=index
	rl.snapLastTerm=rl.taillog[idx].Term
	rl.snapshot=snapshot

	newLog:=make([]LogEntry,0,rl.size()-rl.snapLastIndex)
	newLog = append(newLog, LogEntry{
		Term: rl.snapLastTerm,
	})

	newLog = append(newLog, rl.taillog[idx+1:]...)
	rl.taillog=newLog

}

func (rl *RaftLog)append(e LogEntry){
	rl.taillog=append(rl.taillog,e)
}

func (rl *RaftLog)appendFrom(logicPrevIndex int,entries []LogEntry){
	rl.taillog=append(rl.taillog[:rl.idx(logicPrevIndex)+1],entries...)
}

func(rl *RaftLog)tail(startIndex int)[]LogEntry{
	if(startIndex>=rl.size()){
		return nil
	}
	return rl.taillog[rl.idx(startIndex):]
}

func (rl *RaftLog)installSnapshot(index int,term int,snapshot []byte){
	rl.snapLastIndex=index
	rl.snapLastTerm=term
	rl.snapshot=snapshot

	newLog:=make([]LogEntry,0,1)
	newLog = append(newLog, LogEntry{
		Term: rl.snapLastTerm,
	})

	rl.taillog=newLog
}