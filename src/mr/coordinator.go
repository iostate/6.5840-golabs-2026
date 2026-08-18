package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type StatusType int

const (
	StatusIdle StatusType = iota
	StatusInProgress
	StatusDone
)

type Task struct {
	Type      TaskType
	Status    StatusType
	StartTime time.Time
	Filename  string
}

type Coordinator struct {
	// Your definitions here.
	mu sync.Mutex

	mapTasks    []Task
	reduceTasks []Task

	nReduce int
	nMap    int

	mapDone    bool
	reduceDone bool
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) RequestTask(args *RequestTaskArgs, reply *RequestTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	// TODO: pick an idle map/reduce task (or Wait/Exit) and fill reply.
	if !c.mapDone {
		for i, t := range c.mapTasks {
			if t.Status == StatusIdle || (t.Status == StatusInProgress && time.Since(t.StartTime) > (10*time.Second)) {
				c.mapTasks[i].Status = StatusInProgress
				c.mapTasks[i].StartTime = time.Now()
				reply.TaskType = MapTask
				reply.TaskID = i
				reply.Filename = t.Filename
				reply.NMap = c.nMap
				reply.NReduce = c.nReduce
				return nil
			}
		}
		// no map task available but not all done yet
		reply.TaskType = WaitTask
		return nil
	}

	if !c.reduceDone {
		for i, t := range c.reduceTasks {
			if t.Status == StatusIdle || (t.Status == StatusInProgress && time.Since(t.StartTime) > 10*time.Second) {
				c.reduceTasks[i].Status = StatusInProgress
				c.reduceTasks[i].StartTime = time.Now()

				reply.TaskType = ReduceTask
				reply.TaskID = i
				reply.NMap = c.nMap
				reply.NReduce = c.nReduce
				return nil
			}

		}
		reply.TaskType = WaitTask
		return nil
	}

	reply.TaskType = ExitTask
	return nil
}

func (c *Coordinator) ReportTask(args *ReportTaskArgs, reply *ReportTaskReply) error {
	// TODO: mark the reported task as complete.
	c.mu.Lock()
	defer c.mu.Unlock()

	switch args.TaskType {
	case MapTask:
		c.mapTasks[args.TaskID].Status = StatusDone
		c.checkMapDone()
	case ReduceTask:
		c.reduceTasks[args.TaskID].Status = StatusDone
		c.checkReduceDone()
	}

	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server(sockname string) {
	rpc.Register(c)
	rpc.HandleHTTP()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatalf("listen error %s: %v", sockname, e)
	}
	go http.Serve(l, nil)
}

func (c *Coordinator) checkMapDone() {
	for _, t := range c.mapTasks {
		if t.Status != StatusDone {
			return
		}
	}

	c.mapDone = true
}

func (c *Coordinator) checkReduceDone() {
	for _, t := range c.reduceTasks {
		if t.Status != StatusDone {
			return
		}
	}
	c.reduceDone = true
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Done when reduces are done
	return c.reduceDone
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(sockname string, files []string, nReduce int) *Coordinator {

	// The workers will talk to the coordinator via RPC.
	// Each worker process will, in a loop, ask the coordinator
	// for a task, read the task's input from one or more files,
	// execute the task, write the task's output to one or more
	// files, and again ask the coordinator for a new task. The
	// coordinator should notice if a worker hasn't completed
	// its task in a reasonable amount of time (for this lab,
	// use ten seconds), and give the same task to a different
	// worker.

	// Your code here.

	c := Coordinator{
		nMap:    len(files),
		nReduce: nReduce,
	}

	// Tasks are the files
	c.mapTasks = make([]Task, len(files))
	for i, f := range files {
		c.mapTasks[i] = Task{
			Type:     MapTask,
			Status:   StatusIdle,
			Filename: f,
		}
	}

	// Reduce tasks based on how many times we want to reduce, specified by nReduce
	c.reduceTasks = make([]Task, nReduce)
	for i := range c.reduceTasks {
		c.reduceTasks[i] = Task{
			Type:   ReduceTask,
			Status: StatusIdle,
		}
	}

	c.server(sockname)
	return &c
}
