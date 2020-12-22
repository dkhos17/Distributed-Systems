package kvraft

import "../labrpc"
import "crypto/rand"
import "math/big"
import "sync"
// import "fmt"

type Clerk struct {
	mu sync.Mutex
	servers []*labrpc.ClientEnd

	leader int
	clerk_id int64
	request_id int64
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
	ck.clerk_id = nrand()
	ck.request_id = -1
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {
	args, reply := ck.GetArgs(key)
	for !(ck.servers[ck.leader].Call("KVServer.Get", &args, &reply) && reply.Err == OK) {
		ck.leader += 1; ck.leader %= len(ck.servers);
	}
	return reply.Value
}

func (ck *Clerk) GetArgs(key string) (GetArgs, GetReply) {
	ck.mu.Lock()
	defer ck.mu.Unlock()
	ck.request_id += 1
	return GetArgs{key, ck.clerk_id, ck.request_id}, GetReply{}
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	args, reply := ck.PutAppendArgs(key, value, op)
	for !(ck.servers[ck.leader].Call("KVServer.PutAppend", &args, &reply) && reply.Err == OK) {
		ck.leader += 1; ck.leader %= len(ck.servers);
	}
}

func (ck *Clerk) PutAppendArgs(key string, value string, op string) (PutAppendArgs, PutAppendReply) {
	ck.mu.Lock()
	defer ck.mu.Unlock()
	ck.request_id += 1
	return PutAppendArgs{key, value, op, ck.clerk_id, ck.request_id}, PutAppendReply{}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
