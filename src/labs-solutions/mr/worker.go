package mr

import "fmt"
import "log"
import "net/rpc"
import "hash/fnv"
import "io/ioutil"
import "time"
import "encoding/json"
import "os"
import "strconv"

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

func doMap(args TodoArgs, reply TodoReply, mapf func(string, string) []KeyValue) {
	if reply.Status != "map" {return}

	contents, _ := ioutil.ReadFile(reply.Filename)
	values := mapf(reply.Filename, string(contents))
	
	X := reply.Task_num

	dest := make(map[string][]KeyValue)

	// configure destination files
	for _, kv := range values {
		Y := strconv.Itoa(ihash(kv.Key) % args.NReduce)

		file := "mr-" + X + "-" + Y + ".json"
		dest[file] = append(dest[file], kv)
	}
	
	// write results in dest. files
	for file, kv := range(dest) {
		f, _ := os.OpenFile(file, os.O_CREATE | os.O_RDWR, 0644)
		enc := json.NewEncoder(f)
		enc.Encode(&kv)
		f.Close()
	}
}

func doReduce(args TodoArgs, reply TodoReply, reducef func(string, []string) string) {
	if reply.Status != "reduce" {return}
	
	dest := make(map[string][]string)

	// configure all data for each file
	for X := 0; X < args.NMap; X++ {
		file := "mr-" +  strconv.Itoa(X) + "-" + reply.Task_num + ".json"

		f, _ := os.OpenFile(file, os.O_CREATE | os.O_RDWR, 0644)
		dec := json.NewDecoder(f)

		var kv_data []KeyValue
		if err := dec.Decode(&kv_data); err != nil {
			continue
		}

		for _, kv := range(kv_data) {
			dest[kv.Key] = append(dest[kv.Key], kv.Value)
		}
		f.Close()
	}

	// write collected data in ofile
	ofile, _ := os.Create("mr-out-" + reply.Task_num + ".txt")
	for key, values := range(dest) {
		output := reducef(key, values)
		fmt.Fprintf(ofile, "%v %v\n", key, output)
	}
	ofile.Close()
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
	args := TodoArgs{}
	call("Master.RegisterWorker", &args, &args)

	for {
		reply := TodoReply{}
		call("Master.GetJob", &args, &reply)

		if reply.Status == "done" {
			return
		}

		if reply.Status == "wait" {
			time.Sleep(1*time.Second)
			continue
		}

		if reply.Status == "map" {
			doMap(args, reply, mapf)
		} else {
			doReduce(args, reply, reducef)
		}

		call("Master.WorkerDone", &reply, &reply)
	}
}

//
// example function to show how to make an RPC call to the master.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Master.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	// fmt.Println(err)
	return false
}
