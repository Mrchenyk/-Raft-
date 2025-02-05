package raft

func (rf *Raft)applyTicker(){
	for !rf.killed(){
		rf.mu.Lock()
		rf.applyCond.Wait()
		entries:=make([]LogEntry,0)
		snapPendingApply:=rf.snapPending

		if(!snapPendingApply){
			for i:=rf.lastApplied+1;i<=rf.commitIndex;i++{
			entries=append(entries,rf.log.at(i))
		}
		}


		rf.mu.Unlock()
		
		if(!snapPendingApply){
			for i,entry:=range entries{
				rf.applyCh<-ApplyMsg{
					Command:entry.Command,
					CommandIndex:i+rf.lastApplied+1,
					CommandValid:entry.CommandValid,
				}
			}
		}else{
			rf.applyCh<-ApplyMsg{
				SnapshotValid:true,
				Snapshot:	  rf.log.snapshot,
				SnapshotTerm: rf.log.snapLastTerm,
				SnapshotIndex:rf.log.snapLastIndex,
			}
		}

		rf.mu.Lock()
		if(!snapPendingApply){
			rf.lastApplied+=len(entries)
		}else{
			rf.lastApplied=rf.log.snapLastIndex
			if rf.commitIndex<rf.lastApplied{
				rf.commitIndex=rf.lastApplied
			}
			rf.snapPending=false
		}
		rf.mu.Unlock()
	}
}