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

const (
	MapType = iota
	ReduceType
	IdleType
)

const (
	QueryOP = iota
	ReportOP
)

type CommArgs struct {
	Operation int
	Type      int
	mapKey    string
	reduceKey int
	imFile    string
}

type CommReply struct {
	Type      int
	mapKey    string
	reduceKey int
	imFiles   []string
	nReduce   int
	applied   bool
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
