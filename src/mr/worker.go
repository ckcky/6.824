package mr

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"runtime"
	"sort"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	log.Println("enter:", funcName())

	// Your worker implementation here.
	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

	task := RequestTaskFromCoordinator()

	switch task.TaskType {
	case "Map":
		mDate, err := ioutil.ReadFile(task.FileName)
		if err != nil {
			log.Fatalf("cannot read %v", task.FileName)
		}
		kva := mapf(task.FileName, string(mDate))
		SaveIntermediate(kva, task.Id, task.NReduce)
		ReportDone(task.Id)
	case "Reduce":
		kva := ReadIntermediateFiles(task.Id, task.NMap)
		sort.Slice(kva, func(i, j int) bool {
			return kva[i].Key < kva[j].Key
		})
		doReduce(task.Id, kva, reducef)
		ReportDone(task.Id)
	case "Wait":
		time.Sleep(time.Second)
	case "Exit":
		break
	}

}

// 批量reduce
func doReduce(reduceID int, kva []KeyValue,
	reducef func(string, []string) string) {
	log.Println("enter:", funcName())
	tmpfile, _ := ioutil.TempFile("", "mr-out-*")

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
		fmt.Fprintf(tmpfile, "%v %v\n", kva[i].Key, output)

		i = j
	}

	tmpfile.Close()
	os.Rename(tmpfile.Name(),
		fmt.Sprintf("mr-out-%d", reduceID))
	log.Println("写入 reduce 文件: mr-out-%d", reduceID)

}

// 读取 map中间文件结果
func ReadIntermediateFiles(id int, nMap int) []KeyValue {
	log.Println("enter:", funcName())

	intermediate := []KeyValue{}

	for mapID := 0; mapID < nMap; mapID++ {
		filename := fmt.Sprintf("mr-%d-%d", mapID, id)
		log.Println("读取 map中间文件:", filename)
		file, err := os.Open(filename)
		if err != nil {
			// 可能 Map 还没完成，直接跳过
			continue
		}

		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break // EOF
			}
			intermediate = append(intermediate, kv)
		}

		file.Close()
	}

	return intermediate
}

// map执行结果写入中间文件
func SaveIntermediate(kva []KeyValue, taskId int, nReduce int) {
	bucket := make([][]KeyValue, nReduce)
	for _, kv := range kva {
		r := ihash(kv.Key) % nReduce
		bucket[r] = append(bucket[r], kv)
	}
	for r := 0; r < nReduce; r++ {
		filename := fmt.Sprintf("mr-%d-%d", taskId, r)
		log.Println("写入 map中间文件:", filename)

		file, _ := os.Create(filename)

		enc := json.NewEncoder(file)
		for _, kv := range bucket[r] {
			enc.Encode(&kv)
		}
		file.Close()
	}
}

// rpc 回报回执
func ReportDone(id int) error {
	log.Println("enter:", funcName())
	args := &ReportTaskArgs{TaskId: id}
	ok := call("Coordinator.ReportTask", &args, &ReportTaskReply{})
	if !ok {
		fmt.Println("RPC call failed")
		return errors.New("RPC call [Coordinator.ReportTask] failed")
	}
	return nil
}

// rpc请求任务
func RequestTaskFromCoordinator() Task {
	log.Println("enter:", funcName())
	args := &GetTaskArgs{}
	reply := &GetTaskReply{}
	ok := call("Coordinator.GetTask", args, reply)
	if !ok {
		fmt.Println("RPC call failed")
	}
	log.Printf("reply:% +v", reply)

	return Task{
		Id:       reply.TaskId,
		FileName: reply.FileNames,
		TaskType: reply.TaskType,
		NReduce:  reply.NReduce,
		NMap:     reply.NMap,
	}
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
	call("Coordinator.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	log.Println("enter:", funcName())
	sockname := coordinatorSock()
	log.Println("coordinatorSock: ", sockname)
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

func funcName() string {
	pc, _, _, _ := runtime.Caller(1)
	return runtime.FuncForPC(pc).Name()
}
