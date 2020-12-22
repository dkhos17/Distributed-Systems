package kvraft

import (
	"../labgob"
	"../labrpc"
	"log"
	"../raft"
	"sync"
	"sync/atomic"
	"time"
	"bytes"
	"fmt"
)

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}


type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Op string
	Key string
	Value string
	Clerk_Id int64
	Request_Id int64
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	been map[int64] int64
	data map[string] string
	applied map[int]chan string
}

func (kv *KVServer) AppendOp(op Op) (Err, string) {
	if index, _, isLeader := kv.rf.Start(op); isLeader {
		kv.mu.Lock()
		if _, ok := kv.applied[index]; !ok {
			kv.applied[index] = make(chan string, 1)
		}
		appliedCh := kv.applied[index]
		kv.mu.Unlock()
		select {
			case val := <- appliedCh:
				return OK, val;
			case <- time.After(300*time.Millisecond):
				return ErrNoKey, ""
		}
	}
	
	return ErrWrongLeader, ""
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	op := Op{"Get", args.Key, "", args.Clerk_Id, args.Request_Id}
	reply.Err, reply.Value = kv.AppendOp(op)
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	op := Op{args.Op, args.Key, args.Value, args.Clerk_Id, args.Request_Id}
	reply.Err, _ = kv.AppendOp(op)
}

func (kv *KVServer) duplicate(op Op) bool {
	defer func() {kv.been[op.Clerk_Id] = op.Request_Id}()
	if Request_Id, count := kv.been[op.Clerk_Id]; count {
		return Request_Id >= op.Request_Id
	}
	return false
}

func (kv *KVServer) update(index int, op Op) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if !kv.duplicate(op) {
		if op.Op == "Put" {
			kv.data[op.Key] = op.Value
		} else if op.Op == "Append" {
			kv.data[op.Key] += op.Value
		}
	}

	if _, ok := kv.applied[index]; !ok {
		kv.applied[index] = make(chan string, 1)
	}
	kv.applied[index] <- kv.data[op.Key]
}

func (kv *KVServer) readSnapshot(data []byte) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	// fmt.Println("kv->readSnapshot")
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var LastIncludedIndex int
	var LastIncludedTerm int
	if d.Decode(&LastIncludedIndex) != nil || d.Decode(&LastIncludedTerm) != nil || d.Decode(&kv.data) != nil || d.Decode(&kv.been) != nil {
		fmt.Println("error", LastIncludedIndex, LastIncludedTerm, kv.data, kv.been)
	}
}

func (kv *KVServer) checkRaftState(index int) {
	if kv.maxraftstate == -1 {return}
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if  kv.rf.RaftStateSize() > kv.maxraftstate {
		// fmt.Println(kv.maxraftstate, "maxraftstate", index,  kv.rf.RaftStateSize())
		w := new(bytes.Buffer)
		e := labgob.NewEncoder(w)
		e.Encode(kv.data)
		e.Encode(kv.been)
		go kv.rf.SaveSnapshot(w.Bytes(), kv.maxraftstate, index)
	}
}

func (kv *KVServer) Apply() {
	for {
		msg := <- kv.applyCh
		if msg.Snapshot != nil && len(msg.Snapshot) > 0 {
			kv.readSnapshot(msg.Snapshot)
		} else {
			kv.update(msg.CommandIndex, msg.Command.(Op))
			kv.checkRaftState(msg.CommandIndex)
		}
	}
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.been = make(map[int64]int64)
	kv.data = make(map[string]string)
	kv.applied = make(map[int]chan string)

	go kv.Apply()
	return kv
}
