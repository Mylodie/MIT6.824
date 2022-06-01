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
	MapKey    string
	ReduceKey int
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
	NReduce int
	ImFiles []string
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
		c.tasks = append(c.tasks, &Task{MapKey: file, status: InitializedTask})
	}
	log.Println("Initialized map phrase")
}
func (c *Coordinator) initReducePhrase() {
	c.tasks = []*Task{}
	for i := 0; i < c.NReduce; i++ {
		c.tasks = append(c.tasks, &Task{ReduceKey: i, status: InitializedTask})
	}
	log.Println("Initialized reduce phrase")
}

func (c *Coordinator) schedule() {
	go func() {
		defer close(c.done)

		core := func() {
			log.Println("Core started")
			for {
				msg := <-c.commCh
				log.Println("Core get msg", msg)
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
							msg.reply.MapKey = task.MapKey
							log.Println("Scheduled map task", task.MapKey)
						} else if c.phrase == ReducePhrase {
							msg.reply.Type = ReduceType
							msg.reply.ReduceKey = task.ReduceKey
							msg.reply.ImFiles = c.ImFiles
							msg.reply.NReduce = c.NReduce
							log.Println("Scheduled reduce task", task.ReduceKey)
						}
					} else {
						msg.reply.Type = IdleType
					}
				} else if msg.args.Operation == ReportOP {
					var task *Task
					if c.phrase == MapPhrase && msg.args.Type == MapType {
						task = c.getTask(func(task *Task) bool {
							return task.MapKey == msg.args.MapKey
						})
						log.Println("Finished map task", task.MapKey)
						if task.status == ScheduledTask {
							task.status = FinishedTask
							c.ImFiles = append(c.ImFiles, msg.args.ImFile)
							msg.reply.Applied = true
						} else {
							msg.reply.Applied = false
						}
					} else if c.phrase == ReducePhrase && msg.args.Type == ReduceType {
						task = c.getTask(func(task *Task) bool {
							return task.ReduceKey == msg.args.ReduceKey
						})
						log.Println("Finished reduce task", task.ReduceKey)
						if task.status == ScheduledTask {
							task.status = FinishedTask
						}
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

func MakeCoordinator(files []string, NReduce int) *Coordinator {
	logfile, _ := os.Create("/var/tmp/mr-master-log")
	log.SetOutput(logfile)

	c := Coordinator{}

	c.NReduce = NReduce
	c.files = append(c.files, files...)
	c.commCh = make(chan CommMsg)
	c.done = make(chan Nil)

	c.server()
	log.Printf("Launched server.\n")

	c.schedule()
	log.Printf("Scheduling tasks...\n")

	return &c
}
