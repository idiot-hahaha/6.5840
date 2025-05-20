package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.

type Empty struct {
}
type LinkArgs struct {
	ID int64
}

type PingArgs struct {
	ID int64
}

type TaskEnum int64

const (
	Sleep TaskEnum = iota
	MapTask
	ReduceTask
	Complete
)

type GetTaskArgs struct {
	ID int64
}

type GetTaskReply struct {
	Type    TaskEnum
	TaskID  int64
	NReduce int64
	NMap    int64
	Inames  []string
}
type FinishMapArgs struct {
	TaskID int64
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
