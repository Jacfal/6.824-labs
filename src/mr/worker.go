package mr

import (
	"fmt"
	"encoding/json"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"
	"github.com/google/uuid"
)

var workerId = uuid.New().String()

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

var emptyTask = Task {
	IsEmpty: true,
}

func logworkermessage(message string) {
	log.Printf("worker: %s -> %s", workerId, message)
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func readFile(filename string) string {
	file, err := os.Open(filename)
	if err != nil {
		logworkermessage(fmt.Sprintf("cannot open %v", filename))
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		logworkermessage(fmt.Sprintf("cannot read %v", filename))
	}
	file.Close()
	return string(content)
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	task := GetTask()

	if task.Operation == mapOperation {
		logworkermessage(fmt.Sprintf("Applying map (id %v) function to file %s", task.OpId, task.Files))
		taskfilename := task.Files[0] // only head file is relevant for map operation

		nreducefiles := make([]*os.File, task.NReduce)
		filenames := make([]string, task.NReduce)
		for i := 0; i < task.NReduce; i++ {
			ifilename := fmt.Sprintf("mr-%v-%v", task.OpId, i)
			filenames[i] = ifilename
			ifile, _ := os.Create(ifilename)
			nreducefiles[i] = ifile
		}

		for _, kv := range mapf(taskfilename, readFile(taskfilename)) {
			currentreducefileindex := ihash(kv.Key) % task.NReduce
			file := nreducefiles[currentreducefileindex]
			enc := json.NewEncoder(file)
			err := enc.Encode(&kv)
			if err != nil {
				logworkermessage(fmt.Sprintf("Error during encoding"))
			}
		}

		// close all
		for i := 0; i < task.NReduce; i++ {
			nreducefiles[i].Close()
		}

		TaskComplete(task, filenames)

	} else if task.Operation == reduceOperation {
		logworkermessage(fmt.Sprintf("Applying reduce (id %v) function to files %s", task.OpId, task.Files))

		kva := []KeyValue{}
		for i := 0; i < (len(task.Files)); i++ {
			file, err := os.Open(task.Files[i])
			if err != nil {
				logworkermessage(fmt.Sprintf("cannot open filename for reduce %v", task.Files[i]))
			} else {
				dec := json.NewDecoder(file)
				for {
					var kv KeyValue
					if err := dec.Decode(&kv); err != nil {
						break
					}
					kva = append(kva, kv)
				}
			}
			file.Close()
		}

		// sort
		logworkermessage("Sorting files")
		sort.Sort(ByKey(kva))

		oname := fmt.Sprintf("mr-out-%v", task.OpId)
		ofile, _ := os.Create(oname)

		// reduce
		i := 0
		for i < len(kva) {
			j := i + 1
			for j < len(kva) && kva[j].Key == kva[i].Key {
				j++
			}
			values := []string{}
			for k := i; k < j; k++ {
				values = append(values, kva[k].Value)
			} 
			output := reducef(kva[i].Key, values)
			
			// save in the correct format
			fmt.Fprintf(ofile, "%v %v\n", kva[i].Key, output)
			i = j
		}

		ofile.Close()
		
		// task has been completed, remove intermediate files
		for i := 0; i < (len(task.Files)); i++ {
			os.Remove(task.Files[i])
		}

		logworkermessage(fmt.Sprintf("Task %s complete, contacting coordinator", task.Id))
		TaskComplete(task, []string{ oname })

	} else {
		logworkermessage(fmt.Sprintf("Nothing to do. Waiting for the next task..."))
		time.Sleep(1000 * time.Millisecond)

	}

	Worker(mapf, reducef) // repeat 
}

func GetTask() Task {
	args := GetTaskArgs{ WorkerId: workerId }
	reply := GetTaskReply{}

	ok := call("Coordinator.CreateTask", &args, &reply)
	if ok {
		return reply.TaskToDo
	} else {
		log.Printf("call failed!")
		return emptyTask
	}
}

func TaskComplete(task Task, outputFiles []string) bool {
	args := TaskCompleteArgs { CompletedTask: task, OutputFiles: outputFiles }
	reply := TaskCompleteReply{}
	return call("Coordinator.TaskComplete", &args, &reply)
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
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
