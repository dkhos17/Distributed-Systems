package mr

import "log"
import "net"
import "os"
import "sync"
import "net/rpc"
import "net/http"
import "time"
import "strconv"
// import "fmt"

type Master struct {
	// Your definitions here.
	map_active map[string]bool
	map_timer map[string]time.Time
	map_done map[string]bool
	map_num map[string]string

	reduce_active map[string]bool
	reduce_timer map[string]time.Time
	reduce_done map[string]bool
	reduce_num map[string]string

	nReduce int
	nMap int

	lock sync.Mutex 
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}


//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

func (m *Master) GetJob(args *TodoArgs, reply *TodoReply) error {
	m.lock.Lock()
	defer m.lock.Unlock()
	
	if !checkDone(m.map_active, m.map_done, m.map_timer) {
		takeJob(reply, "map", m.map_active, m.map_done, m.map_num, m.map_timer)
		return nil
	}
	
	if !checkDone(m.reduce_active, m.reduce_done, m.reduce_timer) {
		takeJob(reply, "reduce", m.reduce_active, m.reduce_done, m.reduce_num, m.reduce_timer)
		return nil
	}	
	reply.Status = "done"
	return nil
}

func takeJob(reply *TodoReply, status string, actives map[string]bool, done map[string]bool, nums map[string]string, timer map[string]time.Time) {
	for task, active := range actives {
		if !active && !done[task] {
			actives[task] = true
			timer[task] = time.Now()
			reply.Status = status
			reply.Filename = task
			reply.Task_num = nums[task]
			return
		}
	}
	reply.Status = "wait"
}

func checkDone(active map[string]bool, done map[string]bool, timer map[string]time.Time) bool {
	ret := true
	for task, done := range done {
		if !done && active[task] && time.Since(timer[task]) >= 10*time.Second {
			active[task] = false
		}
		ret = done && ret
	}
	
	return ret
}

func (m *Master) RegisterWorker(args *TodoArgs, reply *TodoArgs) error {
	m.lock.Lock()
	defer m.lock.Unlock()
	reply.NReduce = m.nReduce
	reply.NMap = m.nMap
	return nil
}

func (m *Master) WorkerDone(args *TodoReply, reply *TodoReply) error {
	m.lock.Lock()
	defer m.lock.Unlock()
	
	if args.Status == "map" {
		m.map_done[args.Filename] = true
	} else {
		m.reduce_done[args.Task_num] = true
	}
	return nil
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	if !checkDone(m.map_active, m.map_done, m.map_timer) {
		return false
	} 
	if !checkDone(m.reduce_active, m.reduce_done, m.reduce_timer) {
		return false
	}
	return true
}

func (m *Master) configureWorkerFiles() {
	for j := 0; j < m.nReduce; j++ {
		for i := 0; i < m.nMap; i++ {
			os.Create("mr-" +  strconv.Itoa(i) + "-" + strconv.Itoa(j) + ".json")
		}
		os.Create("mr-out-" + strconv.Itoa(j) + ".txt")
	}
}

func (m *Master) initMaster(files []string, nReduce int) {
	m.nMap, m.nReduce = len(files), nReduce

	m.map_active, m.map_timer = make(map[string]bool), make(map[string]time.Time)
	m.map_done, m.map_num = make(map[string]bool), make(map[string]string)

	m.reduce_active, m.reduce_timer = make(map[string]bool), make( map[string]time.Time)
	m.reduce_done, m.reduce_num = make(map[string]bool), make(map[string]string)

	m.configureWorkerFiles()

	// configure map jobs
	for i, file := range files {
		m.map_num[file] = strconv.Itoa(i)
		m.map_done[file] = false
		m.map_active[file] = false
	}

	// configure reduce jobs
	for i := 0; i < nReduce; i++ {
		i_str := strconv.Itoa(i)
		m.reduce_num[i_str] = i_str
		m.reduce_done[i_str] = false
		m.reduce_active[i_str] = false
	}
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}
	m.initMaster(files, nReduce)
	m.server()
	return &m
}
