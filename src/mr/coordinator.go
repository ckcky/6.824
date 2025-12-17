package mr

import (
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
	TaskType string // "Map" "Reduce"
	Id       int
	Status   TaskStatus
	Start    time.Time
	FileName string //Map task
	NReduce  int
	NMap     int
}
type Coordinator struct {
	// Your definitions here.
	mutex      sync.Mutex
	Maptask    []Task
	ReduceTask []Task
	NReduce    int
	NMap       int
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

			reply.TaskType = "Map"
			reply.TaskId = task.Id
			reply.FileNames = task.FileName
			reply.NReduce = c.NReduce
			reply.NMap = c.NMap

			task.Status = Running
			MyPrintf("返回map任务 %+v\n", *reply)

			return nil
		}

	}

	//检查map任务是否完成
	for _, task := range c.Maptask {
		if task.Status != Completed {
			reply.TaskType = "Wait"
			MyPrintf("仍然有未完成的任务 %+v\n", task)
			return nil
		}
	}

	//分配Reduce任务
	for i := range c.ReduceTask {
		task := &c.ReduceTask[i]
		if task.Status == Idle || task.Status == Running && time.Since(task.Start) > 10*time.Second {
			task.Status = Running
			task.Start = time.Now()

			reply.TaskType = "Reduce"
			reply.TaskId = task.Id
			reply.FileNames = task.FileName
			reply.NReduce = c.NReduce
			reply.NMap = c.NMap
			task.Status = Running
			MyPrintf("分配reduce任务 %+v\n", task)
			return nil
		}
	}

	//检查Reduce 任务是否完成
	for _, task := range c.ReduceTask {
		if task.Status != Completed {
			reply.TaskType = "Wait"
			MyPrintf("仍然有未完成的Reduce任务 %+v\n", task)
			return nil
		}
	}

	reply.TaskType = "Done"
	return nil
}

// 回执
func (c *Coordinator) ReportTask(args *ReportTaskArgs, reply *ReportTaskReply) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	MyPrintf("回报回执%s  返回map任务 %+v \n", args.TaskType, *args)
	switch args.TaskType {
	case "Map":
		task := &c.Maptask[args.TaskId]
		if task.Status == Running {
			task.Status = Completed
		}
		MyPrintf("map子任务完成 +%v \n", task)
	case "Reduce":
		task := &c.ReduceTask[args.TaskId]
		if task.Status == Running {
			task.Status = Completed
		}
		MyPrintf("Reduce 子任务完成 +%v \n", task)
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
		//fmt.Println("加载Map任务 %d", i)
		c.Maptask = append(c.Maptask, Task{
			Id:       i,
			Status:   Idle,
			Start:    time.Now(),
			FileName: file,
		})
	}

	//加载reduce任务
	for i := 0; i < nReduce; i++ {
		//fmt.Println("加载Reduce任务 %d", i)
		c.ReduceTask = append(c.ReduceTask, Task{
			Id:     i,
			Status: Idle,
			Start:  time.Now(),
		})
	}

	MyPrintf("Coordinator 加载完成 +%v \n", c)

	c.NReduce = nReduce
	c.NMap = len(files)
	c.server()
	return &c
}
