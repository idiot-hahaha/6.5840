package mr

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"sort"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

var threadID int64

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

	threadID = int64(os.Getpid())

	link()

	closeHeartBeat := HeartBeat()
	defer closeHeartBeat()

	for {
		var TaskInfo GetTaskReply
		var args = GetTaskArgs{
			ID: threadID,
		}
		if ok := call("Coordinator.GetTask", &args, &TaskInfo); !ok {
			break
		}
		if TaskInfo.Type == MapTask {
			doMap(mapf, TaskInfo.TaskID, TaskInfo.Inames[0], TaskInfo.NReduce)
			finishMap(TaskInfo.TaskID)
		} else if TaskInfo.Type == ReduceTask {
			doReduce(reducef, TaskInfo.TaskID, TaskInfo.NMap)
			finishReduce(TaskInfo.TaskID)
		} else if TaskInfo.Type == Complete {
			break
		} else {
			time.Sleep(time.Second)
		}
	}

}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}

func link() {
	args := &LinkArgs{
		ID: threadID,
	}
	reply := &Empty{}

	retryTime := 0
	for !call("Coordinator.Link", args, reply) && retryTime < 3 {
		retryTime++
	}
	if retryTime >= 3 {
		panic("too many retries")
	}
}

func HeartBeat() func() {
	args := &PingArgs{
		ID: threadID,
	}
	reply := &Empty{}
	ticker := time.NewTicker(HeartbeatInterval)
	go func() {
		for range ticker.C {
			call("Coordinator.Ping", args, reply)
		}
	}()
	return func() {
		ticker.Stop()
	}
}

func doMap(mapf func(string, string) []KeyValue, taskID int64, fileName string, nReduce int64) {
	file, err := os.Open(fileName)
	if err != nil {
		log.Fatalf("cannot open %v", fileName)
	}
	content, err := io.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", fileName)
	}
	file.Close()
	kva := mapf(fileName, string(content))
	buckets := make([][]KeyValue, nReduce)
	for _, kv := range kva {
		bucketsIndex := ihash(kv.Key) % 10
		buckets[bucketsIndex] = append(buckets[bucketsIndex], kv)
	}
	for idx, bucket := range buckets {
		ofile, _ := os.CreateTemp("", "out")
		enc := json.NewEncoder(ofile)
		for _, kv := range bucket {
			err := enc.Encode(&kv)
			if err != nil {
				panic(err)
				return
			}
		}
		ofile.Close()
		oname := mapOutFilePath(taskID, int64(idx))
		os.Rename(ofile.Name(), oname)
	}

}

func finishMap(taskID int64) {
	args := FinishMapArgs{
		TaskID: taskID,
	}
	reply := Empty{}
	call("Coordinator.FinishMap", &args, &reply)
}

type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

func doReduce(reducef func(string, []string) string, taskID int64, nMap int64) {

	intermediate := []KeyValue{}
	for mTaskID := int64(0); mTaskID < nMap; mTaskID++ {
		iname := mapOutFilePath(int64(mTaskID), taskID)
		ifile, _ := os.Open(iname)
		dec := json.NewDecoder(ifile)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}
		ifile.Close()
	}
	ofile, _ := os.CreateTemp("", "tempOut")

	//
	// call Reduce on each distinct key in intermediate[],
	// and print the result to mr-out-0.
	//

	sort.Sort(ByKey(intermediate))
	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}

	ofile.Close()
	oname := fmt.Sprintf("mr-out-%d", taskID)
	os.Rename(ofile.Name(), oname)

}

func finishReduce(taskID int64) {

}
