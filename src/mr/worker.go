package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key
type byKey []KeyValue

func (a byKey) Len() int           { return len(a) }
func (a byKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a byKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

var coordSockName string // socket for coordinator

// main/mrworker.go calls this function.
func Worker(sockname string, mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	coordSockName = sockname
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		return
	}
	defer c.Close()

	for {
		args := RequestTaskArgs{}
		reply := RequestTaskReply{}

		if err := c.Call("Coordinator.RequestTask", &args, &reply); err != nil {
			return // coordinator gone, job done
		}

		switch reply.TaskType {
		case MapTask:
			doMap(mapf, reply)
			CallReportTask(MapTask, reply.TaskID)
		case ReduceTask:
			doReduce(reducef, reply)
			CallReportTask(ReduceTask, reply.TaskID)
		case WaitTask:
			time.Sleep(200 * time.Millisecond)
		case ExitTask:
			return
		}

	}

	CallRequestTask()

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

func CallRequestTask() RequestTaskReply {
	args := RequestTaskArgs{}
	reply := RequestTaskReply{}
	ok := call("Coordinator.RequestTask", &args, &reply)
	if !ok {
		reply.TaskType = ExitTask
	}
	return reply
}

func CallReportTask(taskType TaskType, taskID int) {
	args := ReportTaskArgs{
		TaskType: taskType,
		TaskID:   taskID,
	}
	reply := ReportTaskReply{}
	call("Coordinator.ReportTask", &args, &reply)
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	c, err := rpc.DialHTTP("unix", coordSockName)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	if err := c.Call(rpcname, args, reply); err == nil {
		return true
	}
	log.Printf("%d: call failed err %v", os.Getpid(), err)
	return false
}

func doMap(mapf func(string, string) []KeyValue, reply RequestTaskReply) {
	content, err := ioutil.ReadFile(reply.Filename)
	if err != nil {
		log.Fatalf("cannot read %v: %v", reply.Filename, err)
	}

	kva := mapf(reply.Filename, string(content))

	buckets := make([][]KeyValue, reply.NReduce)
	for _, kv := range kva {
		b := ihash(kv.Key) % reply.NReduce
		buckets[b] = append(buckets[b], kv)
	}

	for y := 0; y < reply.NReduce; y++ {
		oname := fmt.Sprintf("mr-%d-%d", reply.TaskID, y)
		tmpfile, err := ioutil.TempFile(".", "mr-tmp-*")
		if err != nil {
			log.Fatalf("cannot create temp file: %v", err)
		}
		enc := json.NewEncoder(tmpfile)
		for _, kv := range buckets[y] {
			if err := enc.Encode(&kv); err != nil {
				log.Fatalf("cannot encode: %v", err)
			}
		}
		tmpfile.Close()
		os.Rename(tmpfile.Name(), oname)
	}
}

func doReduce(reducef func(string, []string) string, reply RequestTaskReply) {
	intermediate := []KeyValue{}

	for m := 0; m < reply.NMap; m++ {
		iname := fmt.Sprintf("mr-%d-%d", m, reply.TaskID)
		file, err := os.Open(iname)
		if err != nil {
			continue // map task produced nothing
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)

		}
		file.Close()
	}

	sort.Sort(byKey(intermediate))

	oname := fmt.Sprintf("mr-out-%d", reply.TaskID)
	tmpFile, err := ioutil.TempFile(".", "mr-tmp-out-*")
	if err != nil {
		log.Fatalf("cannot create file: %v", err)
	}

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
		fmt.Fprintf(tmpFile, "%v %v\n", intermediate[i].Key, output)
		i = j
	}

	tmpFile.Close()
	os.Rename(tmpFile.Name(), oname)

}
