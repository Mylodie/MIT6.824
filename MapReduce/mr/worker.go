package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"reflect"
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
	rand.Seed(time.Now().UnixNano())
	return 1000 + rand.Intn(9000)
}

var WorkId = rand4num()

func Worker(mapF func(string, string) []KeyValue, reduceF func(string, []string) string) {
	logfile, _ := os.Create(fmt.Sprintf("/var/tmp/mr-worker-%v-log", WorkId))
	log.SetOutput(logfile)
	log.Println("Start working...")

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
	log.Println("Begin query")
	args := &CommArgs{Operation: QueryOP}
	reply := &CommReply{}
	ok := call("Coordinator.Communicate", args, reply)
	log.Println("Query result", reply, ok)
	return reply, ok
}

func reportMap(MapKey string, ImFile string) bool {
	args := &CommArgs{Operation: ReportOP, Type: MapType, MapKey: MapKey, ImFile: ImFile}
	reply := &CommReply{}

	if ok := call("Coordinator.Communicate", args, reply); ok {
		return reply.Applied
	} else {
		return false
	}
}

func reportReduce(ReduceKey int) {
	args := &CommArgs{Operation: ReportOP, Type: ReduceType, ReduceKey: ReduceKey}
	reply := &CommReply{}
	call("Coordinator.Communicate", args, reply)
	return
}

func doMapTask(mapF func(string, string) []KeyValue, reply *CommReply) {
	log.Println("Begin map task", reply.MapKey)
	file, err := os.Open(reply.MapKey)
	if err != nil {
		log.Printf("fail to open %v\n", reply.MapKey)
		return
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Printf("fail to read %v\n", reply.MapKey)
		return
	}
	file.Close()

	kva := mapF(reply.MapKey, string(content))

	ImFile := fmt.Sprintf("mr-im-%d.json", rand4num())
	if f, err := os.Create(ImFile); err == nil {
		enc := json.NewEncoder(f)
		enc.Encode(&kva)

	} else {
		log.Printf("fail to create %v\n", ImFile)
		return
	}

	if ok := reportMap(reply.MapKey, ImFile); !ok {
		os.Remove(ImFile)
	}
	log.Println("Finished map task", reply.MapKey)
}

func doReduceTask(reduceF func(string, []string) string, reply *CommReply) {
	mp := make(map[string][]string)

	log.Println("reply.ImFiles:", reply.ImFiles, reflect.TypeOf(reply.ImFiles))

	for i, i2 := range reply.ImFiles {
		log.Println("ImFiles: ", i, i2)
	}

	for _, ImFile := range reply.ImFiles {
		log.Println(ImFile)
		f, err := os.Open(ImFile)
		if err != nil {
			log.Println("fail to open", ImFile)
			continue
		}
		dec := json.NewDecoder(f)
		kva := []KeyValue{}
		if err := dec.Decode(&kva); err != nil {
			break
		}
		for _, kv := range kva {
			if ihash(kv.Key)%reply.NReduce == reply.ReduceKey {
				mp[kv.Key] = append(mp[kv.Key], kv.Value)
			}
		}

	}

	outFile := fmt.Sprintf("mr-out-%d.txt", reply.ReduceKey)
	f, err := os.Create(outFile)
	if err != nil {
		log.Printf("fail to create %v\n", f)
		return
	}
	for k, v := range mp {
		f.WriteString(fmt.Sprintf("%v %v\n", k, reduceF(k, v)))
	}
	f.Close()

	reportReduce(reply.ReduceKey)

}

func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	for i := 0; i < 3; i++ {
		err = c.Call(rpcname, args, reply)
		if err == nil {
			return true
		}
	}

	log.Println(err)
	return false
}
