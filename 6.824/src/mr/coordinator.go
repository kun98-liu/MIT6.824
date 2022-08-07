package mr

import (
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

//Coordinator should be resposible for assigning tasks and files to mrapp
type Coordinator struct {
	// Your definitions here.
	mu sync.Mutex //lock for the struct

	mapTask_Unallocated    map[int]bool
	mapTask_Undone         map[int]bool
	reduceTask_Unallocated map[int]bool
	reduceTask_Undone      map[int]bool

	files []string

	num_reducer int //reducer的个数，在MakeColrdinator里设置,被 main/mrcoordinator call
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
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
	ret := false
	// Your code here.

	//lock when operate on the maps of task info
	c.mu.Lock()
	defer c.mu.Unlock()

	if len(c.mapTask_Unallocated) == 0 && len(c.mapTask_Undone) == 0 && len(c.reduceTask_Unallocated) == 0 && len(c.reduceTask_Undone) == 0 {
		ret = true
	}

	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	c.files = files
	c.mapTask_Unallocated = make(map[int]bool)
	c.mapTask_Undone = make(map[int]bool)
	c.reduceTask_Unallocated = make(map[int]bool)
	c.reduceTask_Undone = make(map[int]bool)
	c.num_reducer = nReduce

	//file -> mapTask
	for i := 0; i < len(files); i++ {
		c.mapTask_Unallocated[i] = true
	}

	// Your code here.

	c.server()
	return &c
}

func (c *Coordinator) AcquireTask(args *AcTaskArgs, reply *AcTaskReply) error {

	//assigne a task to reply
	c.mu.Lock()
	defer c.mu.Unlock()

	if len(c.mapTask_Unallocated) > 0 {
		for i := range c.mapTask_Unallocated {
			reply.Task_id = i
			reply.Task_t = MapTask
			reply.Map_file_name = c.files[i]
			reply.Num_reducer = c.num_reducer

			delete(c.mapTask_Unallocated, i)
			c.mapTask_Undone[i] = true

			fmt.Printf("Coordinator: Assigned map task %v\n", i)

			//check whether finished in timeout
			go func(task_id int) {
				time.Sleep(10 * time.Second)
				c.mu.Lock()
				defer c.mu.Unlock()
				_, ok := c.mapTask_Undone[task_id]
				if ok {
					// timeout! should put back to no allocated queue
					fmt.Println("Coordinator: Map Task", task_id, "time out!")
					delete(c.mapTask_Undone, task_id)
					c.mapTask_Unallocated[task_id] = true
				}
			}(i)

			return nil
		}
	} else if len(c.mapTask_Undone) > 0 {
		//should wait
		reply.Task_t = WaitTask
		return nil

	} else if len(c.reduceTask_Unallocated) > 0 {
		for i := range c.reduceTask_Unallocated {
			reply.Task_id = i
			reply.Task_t = ReduceTask
			reply.Map_task_num = len(c.files)

			delete(c.reduceTask_Unallocated, i)
			c.reduceTask_Undone[i] = true
			fmt.Printf("Coordinator: Assigned reduce task %v\n", i)

			//check whether finished in timeout
			go func(task_id int) {
				time.Sleep(10 * time.Second)
				c.mu.Lock()
				defer c.mu.Unlock()
				_, ok := c.reduceTask_Undone[task_id]
				if ok {
					// timeout! should put back to no allocated queue
					fmt.Println("Coordinator: Reduce Task", task_id, "time out!")
					delete(c.reduceTask_Undone, task_id)
					c.reduceTask_Unallocated[task_id] = true
				}
			}(i)

			return nil
		}

	} else if len(c.reduceTask_Undone) > 0 {
		//should wait
		reply.Task_t = WaitTask
		return nil
	}

	//all maps are empty, return a err to stop the worker
	return errors.New("Coordinator: No more task to be assigned. All job done")
}

func (c *Coordinator) TaskDone(args *DoneTaskArgs, reply *DoneTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if args.Task_t == MapTask {
		_, ok := c.mapTask_Undone[args.Task_id]
		if ok {
			// the task is in the NoDone queue
			delete(c.mapTask_Undone, args.Task_id)

		} else {
			_, ok := c.mapTask_Unallocated[args.Task_id]
			if ok {
				// the taskid is found in Unallocated, this can happen when this is timeout and then send reply
				delete(c.mapTask_Unallocated, args.Task_id)
			}
		}

		fmt.Printf("Coordinator: map task %v done\n", args.Task_id)

		// check whether all map task is done and then begin reduce task
		if len(c.mapTask_Unallocated) == 0 && len(c.mapTask_Undone) == 0 {
			for i := 0; i < c.num_reducer; i++ {
				c.reduceTask_Unallocated[i] = true
			}
		}
	} else {
		_, ok := c.reduceTask_Undone[args.Task_id]
		if ok {
			delete(c.reduceTask_Undone, args.Task_id)
		} else {
			_, ok := c.reduceTask_Unallocated[args.Task_id]
			if ok {
				delete(c.reduceTask_Unallocated, args.Task_id)
			}
		}
		fmt.Printf("Coordinator: reduce task %v done\n", args.Task_id)
	}
	return nil

}
