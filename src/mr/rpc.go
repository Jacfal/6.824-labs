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

// Add your RPC definitions here.

const (
	wait = 0
	mapOperation = 1
	reduceOperation = 2
	shutdown = 3
)

type Task struct {
	Id 				string
	IsEmpty   bool
	Operation int
	Files 		[]string
	NReduce   int
	OpId			int
	startTime int64
}

type GetTaskArgs struct {
	//Name string
	WorkerId string
}

type GetTaskReply struct {
	TaskToDo Task
}

type TaskCompleteArgs struct {
	CompletedTask Task
	OutputFiles []string
}

type TaskCompleteReply struct {}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
