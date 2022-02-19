package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strings"
	"sync"
	"strconv"
	"io/ioutil"
	"time"
	"github.com/google/uuid"
)

func init() {
	debug := false // run with log messages at the output
	if (!debug) {
		log.SetOutput(ioutil.Discard)
	}
}

type Coordinator struct {
	// Your definitions here.
	mu              	sync.Mutex
	filesToMap      	[]string
	filesToReduce   	[]string
	runningTasks 			[]Task
	nReduce         	int
}

// Your code here -- RPC handlers for the worker to call.

// next id for operation
var nextMapOpId = 0  
var nextRedOpId = 0

var mapComplete = false
var reduceComplete = false

// helper functions
func removeTaskFromProcessing(t []Task, id string) []Task {
	for i := 0; i < len(t); i++ {
		if t[i].Id == id {
			t[i] = t[len(t)-1]
			return t[:len(t)-1]
		}
	}
	return t
}

func getExpiredTasks(t []Task) Task {
	for i := 0; i < len(t); i++ {
		expirationTime := t[i].startTime + 10 // expiry when start time is older than 10 sec
		if (expirationTime < time.Now().Unix()) {
			return t[i]
		}
	}
	return emptyTask
}

func (c *Coordinator) TaskComplete(args *TaskCompleteArgs, reply *TaskCompleteReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	
	if args.CompletedTask.Operation == mapOperation {
		// MAP Operation finished
		c.runningTasks = removeTaskFromProcessing(c.runningTasks, args.CompletedTask.Id)

		// add files to reduce files
		c.filesToReduce = append(c.filesToReduce, args.OutputFiles...)

		if (len(c.filesToMap) == 0 && len(c.runningTasks) == 0) {
			log.Print("MAP complete")
			mapComplete = true
		}
	} else if args.CompletedTask.Operation == reduceOperation {
		// REDUCE Operation finished
		c.runningTasks = removeTaskFromProcessing(c.runningTasks, args.CompletedTask.Id)
		
		if (len(c.filesToReduce) == 0 && len(c.runningTasks) == 0) {
			log.Print("REDUCE complete")
			reduceComplete = true
		}
	}

	return nil
}

func (c *Coordinator) CreateTask(args *GetTaskArgs, reply *GetTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if len(c.filesToMap) > 0 {
		// Get worker map task to processing
		log.Printf("Creating map task for worker %v", args.WorkerId)

		filesToProcessing := []string { c.filesToMap[0] }
		c.filesToMap = c.filesToMap[1:] // tail
		log.Printf("Sending file %s to map (id %v) processing to worker-%v", filesToProcessing, nextMapOpId, args.WorkerId)

		newTask := Task{
			Id: 			 uuid.New().String(),
			IsEmpty:   false,
			Operation: mapOperation,
			Files: 		 filesToProcessing,
			NReduce:   c.nReduce,
			OpId: 		 nextMapOpId,	
			startTime: time.Now().Unix(),		
		}

		c.runningTasks = append(c.runningTasks, newTask)
		reply.TaskToDo = newTask
		nextMapOpId++ 

	} else if nextRedOpId < c.nReduce && mapComplete {
		// Get worker reduce task to processing
		log.Printf("Creating reduce task for worker %v", args.WorkerId)
		
		filesToProcessing := []string{}

		for i := 0; i < len(c.filesToReduce); i++ {
			// get first key
			currentFileNameSplit := strings.Split(c.filesToReduce[i], "-")
			currentKey, err := strconv.Atoi(currentFileNameSplit[len(currentFileNameSplit)-1])
			if err != nil {
				log.Fatal(err)
			}

			if currentKey == nextRedOpId {
				filesToProcessing = append(filesToProcessing, c.filesToReduce[i])
			}
		}
		log.Printf("Sending files %s to reduce (id %v) processing to worker-%v", filesToProcessing, nextRedOpId, args.WorkerId)

		// remove reduce files from queue
		for i := 0; i < len(filesToProcessing); i++ {
			for j := 0; j < len(c.filesToReduce); j++ {
				if (filesToProcessing[i] == c.filesToReduce[j]) {
					c.filesToReduce[j] = c.filesToReduce[len(c.filesToReduce)-1]
					c.filesToReduce = c.filesToReduce[:len(c.filesToReduce)-1]
				}
			}
		}

		newTask := Task{
			Id: 			 uuid.New().String(),
			IsEmpty:   false,
			Operation: reduceOperation,
			Files: 		 filesToProcessing,
			NReduce:   c.nReduce,
			OpId: 		 nextRedOpId,
			startTime: time.Now().Unix(),
		}

		c.runningTasks = append(c.runningTasks, newTask)
		reply.TaskToDo = newTask
		nextRedOpId++
	} else {

		// Nothing to do - wait for the next task 
		expired := getExpiredTasks(c.runningTasks)

		if (!expired.IsEmpty) {
			log.Printf("Task %s expired. Getting task to the worker %s for processing", expired.Id, args.WorkerId)
			
			// reset timestamp
			for i := 0; i < len(c.runningTasks); i++ {
				if (c.runningTasks[i].Id == expired.Id) {
					c.runningTasks[i].startTime = time.Now().Unix()
					break
				}
			}
			
			reply.TaskToDo = expired
		} else {
			reply.TaskToDo = Task{
				IsEmpty:   false,
				Operation: wait,
				Files: 		 []string {},
				NReduce:   c.nReduce,
			}
		}
	}
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	c.mu.Lock()	
	defer c.mu.Unlock()
	
	done := false

	if mapComplete && reduceComplete {
		log.Println("All tasks completed. Ending coordinator...")
		done = true
	}
	return done
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {	
	// Your code here.
	c := Coordinator{
		filesToMap:      files,
		filesToReduce:   []string{},
		runningTasks: 	 []Task{},
		nReduce:         nReduce}

	c.server()

	log.Println("Coordinator ready for work!")
	return &c
}
