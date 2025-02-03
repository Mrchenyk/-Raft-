package raft

func (rf *Raft)applyTicker(){
	for !rf.killed(){
		rf.mu.Lock()
		rf.applyCond.Wait()

		entries:=make([]LogEntry,0)
		for i:=rf.lastApplied+1;i<=rf.commitIndex;i++{
			entries=append(entries,rf.log[i])
		}


		rf.mu.Unlock()
		
		for i,entry:=range entries{
			rf.applyCh<-ApplyMsg{
				Command:entry.Command,
				CommandIndex:i+rf.lastApplied+1,
				CommandValid:entry.CommandValid,
			}
		}

		rf.mu.Lock()
		rf.lastApplied+=len(entries)
		rf.mu.Unlock()
	}
}