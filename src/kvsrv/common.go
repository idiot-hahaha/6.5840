package kvsrv

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ClientID int64
	UniqueID uint64
}

type PutAppendReply struct {
	Value string
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	ClientID int64
	UniqueID uint64
}

type GetReply struct {
	Value string
}

type ReleaseCacheArgs struct {
	ClientID int64
	Ack      []uint64
}

type ReleaseCacheReply struct {
}
