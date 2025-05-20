package kvsrv

import (
	"6.5840/labrpc"
	"crypto/rand"
	"math/big"
	"sync"
	"sync/atomic"
)

type Clerk struct {
	server *labrpc.ClientEnd
	// You will have to modify this struct.
	opIndex  atomic.Uint64
	clientID int64
	ackMu    sync.Mutex
	AckPool  map[uint64]struct{} // 用于清理服务器缓存
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(server *labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.server = server
	// You'll have to add code here.
	ck.opIndex.Store(0)
	ck.clientID = nrand()
	ck.AckPool = make(map[uint64]struct{})
	return ck
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.server.Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {

	// You will have to modify this function.
	DPrintf("Get %s", key)
	args := GetArgs{
		Key:      key,
		ClientID: ck.clientID,
		UniqueID: ck.opIndex.Add(1),
		Ack:      make([]uint64, 0),
	}
	ck.ackMu.Lock()
	for ack, _ := range ck.AckPool {
		args.Ack = append(args.Ack, ack)
	}
	ck.ackMu.Unlock()
	defer func() {
		ck.ackMu.Lock()
		ck.AckPool[args.UniqueID] = struct{}{}
		ck.ackMu.Unlock()
	}()
	reply := GetReply{}
	if ck.CallWithRetry("KVServer.Get", &args, &reply) {
		ck.ackMu.Lock()
		for _, ack := range args.Ack {
			delete(ck.AckPool, ack)
		}
		ck.ackMu.Unlock()
		return reply.Value
	}
	return ""
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.server.Call("KVServer."+op, &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) string {
	// You will have to modify this function.
	args := PutAppendArgs{
		Key:      key,
		Value:    value,
		ClientID: ck.clientID,
		UniqueID: ck.opIndex.Add(1),
		Ack:      make([]uint64, 0),
	}

	ck.ackMu.Lock()
	for ack, _ := range ck.AckPool {
		args.Ack = append(args.Ack, ack)
	}
	ck.ackMu.Unlock()

	defer func() {
		ck.ackMu.Lock()
		ck.AckPool[args.UniqueID] = struct{}{}
		ck.ackMu.Unlock()
	}()

	reply := PutAppendReply{}
	switch op {
	case "Put":
		if ck.CallWithRetry("KVServer.Put", &args, &reply) {
			ck.ackMu.Lock()
			for _, ack := range args.Ack {
				delete(ck.AckPool, ack)
			}
			ck.ackMu.Unlock()
			return reply.Value
		}
		DPrintf("Call KVServer.Put failed!")
		return ""
	case "Append":
		if ck.CallWithRetry("KVServer.Append", &args, &reply) {

			ck.ackMu.Lock()
			for _, ack := range args.Ack {
				delete(ck.AckPool, ack)
			}
			ck.ackMu.Unlock()
			return reply.Value
		}
		DPrintf("Call KVServer.Append failed!")
		return ""
	default:
		panic("Undefined operation!")
	}
}

func (ck *Clerk) Put(key string, value string) {
	DPrintf("Put %s:%s", key, value)
	ck.PutAppend(key, value, "Put")
}

// Append value to key's value and return that value
func (ck *Clerk) Append(key string, value string) string {
	DPrintf("Append %s:%s", key, value)
	return ck.PutAppend(key, value, "Append")
}

func (ck *Clerk) CallWithRetry(path string, args interface{}, reply interface{}) bool {
	retryTimes := 0
	for !ck.server.Call(path, args, reply) {
		retryTimes++
		if retryTimes >= 300 {
			return false
		}
		DPrintf("Call %s failed!, retry", path)
	}
	return true
}
