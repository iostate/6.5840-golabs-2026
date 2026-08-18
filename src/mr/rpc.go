package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

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

type TaskType int

const (
	MapTask TaskType = iota
	ReduceTask
	WaitTask
	ExitTask
)

type RequestTaskArgs struct {
}

type RequestTaskReply struct {
	TaskType TaskType
	TaskID   int
	Filename string // input file for a map task
	NMap     int
	NReduce  int
}

type ReportTaskArgs struct {
	TaskType TaskType
	TaskID   int
}

type ReportTaskReply struct {
}
