package mr

import (
	"log"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type Nil struct{}

const (
	MapPhrase = iota
	ReducePhrase
)

const (
	InitializedTask = iota
	ScheduledTask
	FinishedTask
)

type Task struct {
	mapKey    string
	reduceKey int
	status    int
	startTime time.Time
}

type CommMsg struct {
	args  *CommArgs
	reply *CommReply
	ok    chan Nil
}

type Coordinator struct {
	files   []string
	nReduce int
	imFiles []string
	phrase  int
	tasks   []*Task
	commCh  chan CommMsg
	done    chan Nil
}

func (c *Coordinator) Communicate(args *CommArgs, reply *CommReply) error {
	msg := CommMsg{args, reply, make(chan Nil)}
	c.commCh <- msg
	<-msg.ok
	return nil
}

func (c *Coordinator) getTask(cond func(*Task) bool) *Task {
	for _, task := range c.tasks {
		if cond(task) {
			return task
		}
	}
	return nil
}

func (c *Coordinator) initMapPhrase() {
	for _, file := range c.files {
		c.tasks = append(c.tasks, &Task{mapKey: file, status: InitializedTask})
	}
}
func (c *Coordinator) initReducePhrase() {
	c.tasks = []*Task{}
	for i := 0; i < c.nReduce; i++ {
		c.tasks = append(c.tasks, &Task{reduceKey: i, status: InitializedTask})
	}
}

func (c *Coordinator) schedule() {
	go func() {
		defer close(c.done)

		core := func() {
			for {
				msg := <-c.commCh
				if msg.args.Operation == QueryOP {
					task := c.getTask(func(task *Task) bool {
						return task.status == InitializedTask ||
							task.status == ScheduledTask && time.Since(task.startTime) > time.Second*10
					})
					if task != nil {
						task.startTime = time.Now()
						task.status = ScheduledTask
						if c.phrase == MapPhrase {
							msg.reply.Type = MapType
							msg.reply.mapKey = task.mapKey
						} else if c.phrase == ReducePhrase {
							msg.reply.Type = ReduceType
							msg.reply.reduceKey = task.reduceKey
							msg.reply.imFiles = c.imFiles
							msg.reply.nReduce = c.nReduce
						}
					} else {
						msg.reply.Type = IdleType
					}
				} else if msg.args.Operation == ReportOP {
					var task *Task
					if c.phrase == MapPhrase && msg.args.Type == MapType {
						task = c.getTask(func(task *Task) bool {
							return task.mapKey == msg.args.mapKey
						})
					} else if c.phrase == ReducePhrase && msg.args.Type == ReduceType {
						task = c.getTask(func(task *Task) bool {
							return task.reduceKey == msg.args.reduceKey
						})
					}
					if task.status == ScheduledTask {
						task.status = FinishedTask
						c.imFiles = append(c.imFiles, msg.args.imFile)
						msg.reply.applied = true
					} else {
						msg.reply.applied = false
					}
				} else {
					log.Printf("Get wrong CommArgs.Operation %d\n", msg.args.Operation)
				}
				msg.ok <- Nil{}

				task := c.getTask(func(task *Task) bool {
					return task.status != FinishedTask
				})
				if task == nil {
					break
				}
			}
		}

		c.phrase = MapPhrase
		c.initMapPhrase()
		core()

		c.phrase = ReducePhrase
		c.initReducePhrase()
		core()
	}()
}

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
	go func() {
		http.Serve(l, nil)
	}()
}

func (c *Coordinator) Done() bool {
	select {
	case _, ok := <-c.done:
		return !ok
	default:
		return false
	}
}

func MakeCoordinator(files []string, nReduce int) *Coordinator {
	logfile, _ := os.Create("/var/tmp/mr-master-log")
	log.SetOutput(logfile)

	c := Coordinator{}

	c.nReduce = nReduce
	c.files = append(c.files, files...)

	c.server()
	log.Printf("Launched server.\n")

	c.schedule()
	log.Printf("Scheduling tasks...\n")

	return &c
}
