package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
)

type Master struct {
	// Your definitions here.
	AllFileNames map[string]int
	ReduceTasks  map[int]int
}

const (
	UnAllocated = iota
	Allocated
	Completed
)

var mapTasks chan string // Channel for posting map task filenames
var reduceTasks chan int // Channel for posting reduce tasks.

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (m *Master) WorkerCallHandler(args *TaskRequest, reply *TaskResponse) error {
	msgType := args.MessageType
	if msgType == RequestWork {
		select {
		case nextFile := <-mapTasks:
			reply.Filename = nextFile
			reply.TaskType = "map"
		case nextReduce := <-reduceTasks:
			fmt.Print(nextReduce)
			reply.Filename = ""
			reply.TaskType = "reduce"
		}
	}

	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	mapTasks = make(chan string, 5)
	reduceTasks = make(chan int, 5)
	rpc.Register(m)
	rpc.HandleHTTP()
	go m.GenerateTask() // Start a groutine that posts tasks to the channels
	// l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	ret := false
	// Needs to check the state of every worker allocated. Done if all are completed or something.

	return ret
}

// This will be called as goroutine to constantly post unallocated tasks to respective channel.
func (m *Master) GenerateTask() error {
	for k, v := range m.AllFileNames {
		if v == UnAllocated {
			fmt.Printf("Posting file to channel %v", k)
			mapTasks <- k
		}
	}

	for k, v := range m.ReduceTasks {
		if v == UnAllocated {
			fmt.Printf("Posting reduce to channel %v", k)
			reduceTasks <- k
		}
	}
	return nil
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}
	m.AllFileNames = make(map[string]int)
	m.ReduceTasks = make(map[int]int)

	for _, name := range files {
		m.AllFileNames[name] = UnAllocated
	}

	for i := 0; i < nReduce; i++ {
		m.ReduceTasks[i] = UnAllocated
	}
	m.server()
	return &m
}
