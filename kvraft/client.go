package kvraft

import "course/labrpc"
import "crypto/rand"
import "math/big"

type Clerk struct {
	servers []*labrpc.ClientEnd
	leaderId int 
	
	// clientID+seqId 确定一个唯一的命令
	clientId int64
	seqId    int64
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.leaderId = 0
	ck.clientId = nrand()
	ck.seqId = 0
	return ck
}

//GET获取数据
func (ck *Clerk) Get(key string) string {
	args := GetArgs{
		Key: key,
	}

	for {
		var reply GetReply
		ok := ck.servers[ck.leaderId].Call("KVServer.Get", &args, &reply)
		if !ok || reply.Err == ErrWrongLeader || reply.Err == ErrTimeout {
			// 请求失败，选择另一个节点重试
			ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
			continue
		}
		// 调用成功，返回 value
		return reply.Value
	}
}


func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	args := PutAppendArgs{
		Key:      key,
		Value:    value,
		Op:       op,
		ClientId: ck.clientId,
		SeqId:    ck.seqId,
	}

	for {
		var reply PutAppendReply
		ok := ck.servers[ck.leaderId].Call("KVServer.PutAppend", &args, &reply)
		if !ok || reply.Err == ErrWrongLeader || reply.Err == ErrTimeout {
			// 请求失败，选择另一个节点重试
			ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
			continue
		}
		// 调用成功，返回
		ck.seqId++
		return
	}
}

//Put设置数据
func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}

//Append追加数据
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
