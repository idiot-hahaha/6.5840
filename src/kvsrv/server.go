package kvsrv

import (
	"log"
	"sync"
)

const Debug = true

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type KVServer struct {
	mu sync.Mutex

	// Your definitions here.
	cacheMux sync.Mutex
	kv       map[string]string
	resCache map[int64]map[uint64]string
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.cacheMux.Lock()
	defer kv.cacheMux.Unlock()
	kv.removeFromCache(args.ClientID, args.Ack)
	if res, ok := kv.checkResCache(args.ClientID, args.UniqueID); ok {
		reply.Value = res
		return
	}

	kv.mu.Lock()
	defer kv.mu.Unlock()
	if value, ok := kv.kv[args.Key]; ok {
		reply.Value = value
	} else {
		reply.Value = ""
	}
	kv.SaveRes(args.ClientID, args.UniqueID, reply.Value)
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.cacheMux.Lock()
	defer kv.cacheMux.Unlock()
	kv.removeFromCache(args.ClientID, args.Ack)
	if res, ok := kv.checkResCache(args.ClientID, args.UniqueID); ok {
		reply.Value = res
		return
	}
	kv.mu.Lock()
	defer kv.mu.Unlock()
	kv.kv[args.Key] = args.Value
	kv.SaveRes(args.ClientID, args.UniqueID, reply.Value)
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.cacheMux.Lock()
	defer kv.cacheMux.Unlock()
	kv.removeFromCache(args.ClientID, args.Ack)
	if res, ok := kv.checkResCache(args.ClientID, args.UniqueID); ok {
		reply.Value = res
		return
	}

	kv.mu.Lock()
	defer kv.mu.Unlock()
	if value, ok := kv.kv[args.Key]; ok {
		reply.Value = value
		kv.kv[args.Key] = value + args.Value
	} else {
		reply.Value = ""
	}
	kv.SaveRes(args.ClientID, args.UniqueID, reply.Value)
}

func StartKVServer() *KVServer {
	kv := new(KVServer)

	// You may need initialization code here.
	kv.kv = make(map[string]string)
	kv.resCache = make(map[int64]map[uint64]string)
	return kv
}

func (kv *KVServer) checkResCache(clientID int64, uniqueID uint64) (res string, ok bool) {
	if m, ok := kv.resCache[clientID]; ok {
		if res, ok = m[uniqueID]; ok {
			return res, ok
		}
	}
	return "", false
}

func (kv *KVServer) SaveRes(clientID int64, uniqueID uint64, value string) {
	if m, ok := kv.resCache[clientID]; ok {
		m[uniqueID] = value
		return
	}
	kv.resCache[clientID] = make(map[uint64]string)
	kv.resCache[clientID][uniqueID] = ""
}

func (kv *KVServer) removeFromCache(clientID int64, ackUniqueIds []uint64) {
	if len(ackUniqueIds) == 0 {
		return
	}
	m, ok := kv.resCache[clientID]
	if !ok {
		return
	}
	for _, ackUniqueId := range ackUniqueIds {
		delete(m, ackUniqueId)
	}
}
