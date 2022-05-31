package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
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

func rand4num() int {
	return 1000 + rand.Intn(9000)
}

var WorkId = rand4num()

func Worker(mapF func(string, string) []KeyValue, reduceF func(string, []string) string) {
	logfile, _ := os.Create(fmt.Sprintf("mr-worker-%v-log", WorkId))
	log.SetOutput(logfile)

	for {
		if reply, ok := query(); ok {
			switch reply.Type {
			case MapType:
				doMapTask(mapF, reply)
			case ReduceType:
				doReduceTask(reduceF, reply)
			case IdleType:
				time.Sleep(1 * time.Second)
			default:
				panic(fmt.Sprintf("unexpected jobType %v", reply.Type))
			}
		} else {
			break
		}
	}
}

func query() (*CommReply, bool) {
	args := &CommArgs{Operation: QueryOP}
	reply := &CommReply{}

	ok := call("Coordinator.Communicate", args, reply)
	return reply, ok
}

func reportMap(mapKey string, imFile string) bool {
	args := &CommArgs{Operation: ReportOP, Type: MapType, mapKey: mapKey, imFile: imFile}
	reply := &CommReply{}

	if ok := call("Coordinator.Communicate", args, reply); ok {
		return reply.applied
	} else {
		return false
	}
}

func reportReduce(reduceKey int) {
	args := &CommArgs{Operation: ReportOP, Type: ReduceType, reduceKey: reduceKey}
	reply := &CommReply{}
	call("Coordinator.Communicate", args, reply)
	return
}

func doMapTask(mapF func(string, string) []KeyValue, reply *CommReply) {
	file, err := os.Open(reply.mapKey)
	if err != nil {
		log.Printf("fail to open %v\n", reply.mapKey)
		return
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Printf("fail to read %v\n", reply.mapKey)
		return
	}
	file.Close()

	kva := mapF(reply.mapKey, string(content))

	imFile := fmt.Sprintf("mr-im-%d.json", rand4num())
	if f, err := os.Create(imFile); err == nil {
		enc := json.NewEncoder(f)
		enc.Encode(&kva)

	} else {
		log.Printf("fail to create %v\n", imFile)
		return
	}

	if ok := reportMap(reply.mapKey, imFile); !ok {
		os.Remove(imFile)
	}

}

func doReduceTask(reduceF func(string, []string) string, reply *CommReply) {
	mp := make(map[string][]string)

	for _, imFile := range reply.imFiles {
		f, err := os.Open(imFile)
		if err != nil {
			log.Printf("fail to open %v\n", imFile)
			return
		}
		dec := json.NewDecoder(f)
		kva := []KeyValue{}
		if err := dec.Decode(&kva); err != nil {
			break
		}
		for _, kv := range kva {
			if ihash(kv.Key)%reply.nReduce == reply.reduceKey {
				mp[kv.Key] = append(mp[kv.Key], kv.Value)
			}
		}

	}

	outFile := fmt.Sprintf("mr-out-%d.txt", reply.reduceKey)
	f, err := os.Create(outFile)
	if err != nil {
		log.Printf("fail to create %v\n", f)
		return
	}
	for k, v := range mp {
		f.WriteString(fmt.Sprintf("%v %v\n", k, reduceF(k, v)))
	}
	f.Close()

	reportReduce(reply.reduceKey)

}

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
