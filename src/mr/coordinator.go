package mr

import (
	"fmt"
	"log"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type TaskStatus int

const (
	Idle TaskStatus = iota
	Running
	Completed
)

type Task struct {
	Id       int
	Status   TaskStatus
	Start    time.Time
	FileName string //Map task
}
type Coordinator struct {
	// Your definitions here.
	mutex      sync.Mutex
	Maptask    []Task
	ReduceTask []Task
	NReduce    int
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// 请求任务
func (c *Coordinator) GetTask(args *GetTaskArgs, reply *GetTaskReply) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	//分配Map任务
	for i := range c.Maptask {
		task := &c.Maptask[i]
		if task.Status == Idle || task.Status == Running && time.Since(task.Start) > 10*time.Second {
			task.Status = Running
			task.Start = time.Now()

			reply = &GetTaskReply{
				TaskType:  "Map",
				TaskId:    task.Id,
				FileNames: task.FileName,
				NReduce:   c.NReduce,
			}
			return nil
		}

	}

	//检查map任务是否完成
	for _, task := range c.Maptask {
		if task.Status != Completed {
			reply = &GetTaskReply{TaskType: "Wait"}
			return nil
		}
	}

	//分配Reduce任务
	for i := range c.ReduceTask {
		task := &c.ReduceTask[i]
		if task.Status == Idle || task.Status == Running && time.Since(task.Start) > 10*time.Second {
			task.Status = Running
			task.Start = time.Now()

			reply = &GetTaskReply{
				TaskType:  "Reduce",
				TaskId:    task.Id,
				FileNames: task.FileName,
				NReduce:   c.NReduce,
			}
		}
	}

	//检查Reduce 任务是否完成
	for _, task := range c.ReduceTask {
		if task.Status != Completed {
			reply = &GetTaskReply{TaskType: "Wait"}
		}
	}

	reply = &GetTaskReply{TaskType: "Done"}
	return nil
}

// 回执
func (c *Coordinator) ReportTask(args *ReportTaskArgs, reply *ReportTaskReply) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	switch args.TaskType {
	case "Map":
		task := &c.Maptask[args.TaskId]
		if task.Status == Running {
			task.Status = Completed
		}

	case "Reduce":
		task := &c.ReduceTask[args.TaskId]
		if task.Status == Running {
			task.Status = Completed
		}
	}
	return nil
}

// start a thread that listens for RPCs from worker.go
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

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	// Your code here.

	//检查map任务
	for _, v := range c.Maptask {
		if v.Status != Completed {
			return false
		}
	}

	//检查reduce任务
	for _, v := range c.ReduceTask {
		if v.Status != Completed {
			return false
		}
	}

	return true
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.

	//加载map任务
	for i, file := range files {
		fmt.Println("加载Map任务 %d", i)
		c.Maptask = append(c.Maptask, Task{
			Id:       i,
			Status:   Idle,
			Start:    time.Time{},
			FileName: file,
		})
	}

	//加载reduce任务
	for i := 0; i < nReduce; i++ {
		c.ReduceTask = append(c.ReduceTask, Task{
			Id:     i,
			Status: Idle,
			Start:  time.Time{},
			// FileName: "",
		})
	}
	c.server()
	return &c
}
