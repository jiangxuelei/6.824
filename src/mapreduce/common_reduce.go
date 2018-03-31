package mapreduce

import (
	"encoding/json"
	"os"
	"sort"
	"strconv"
	"strings"
)

func doReduce(
	jobName string, // the name of the whole MapReduce job
	reduceTask int, // which reduce task this is
	outFile string, // write the output here
	nMap int, // the number of map tasks that were run ("M" in the paper)
	reduceF func(key string, values []string) string,
) {
	//
	// doReduce manages one reduce task: it should read the intermediate
	// files for the task, sort the intermediate key/value pairs by key,
	// call the user-defined reduce function (reduceF) for each key, and
	// write reduceF's output to disk.
	//
	// You'll need to read one intermediate file from each map task;
	// reduceName(jobName, m, reduceTask) yields the file
	// name from map task m.
	//
	// Your doMap() encoded the key/value pairs in the intermediate
	// files, so you will need to decode them. If you used JSON, you can
	// read and decode by creating a decoder and repeatedly calling
	// .Decode(&kv) on it until it returns an error.
	//
	// You may find the first example in the golang sort package
	// documentation useful.
	//
	// reduceF() is the application's reduce function. You should
	// call it once per distinct key, with a slice of all the values
	// for that key. reduceF() returns the reduced value for that key.
	//
	// You should write the reduce output as JSON encoded KeyValue
	// objects to the file named outFile. We require you to use JSON
	// because that is what the merger than combines the output
	// from all the reduce tasks expects. There is nothing special about
	// JSON -- it is just the marshalling format we chose to use. Your
	// output code will look something like this:
	//
	// enc := json.NewEncoder(file)
	// for key := ... {
	// 	enc.Encode(KeyValue{key, reduceF(...)})
	// }
	// file.Close()
	//
	// Your code here (Part I).
	//
	kvs := SortKV{}
	var kv KeyValue
	for m := 0; m < nMap; m++ {
		reduceName := reduceName(jobName, m, reduceTask)
		f, _ := os.Open(reduceName)
		enc := json.NewDecoder(f)

		for {
			if e := enc.Decode(&kv); e == nil {
				kvs = append(kvs, kv)
			} else {
				break
			}
		}
		f.Close()
	}
	sort.Sort(kvs)
	fristKey := ""
	tmpList := make([]string, 0)
	for _, kv := range kvs {
		if strings.Compare(fristKey, "") == 0{
			fristKey = kv.Key
			tmpList = append(tmpList, kv.Value)
		} else if strings.Compare(fristKey, kv.Key) == 0{
			tmpList = append(tmpList, kv.Value)
		} else {
			output := reduceF(fristKey, tmpList)
			writeOutputFile(mergeName(jobName, reduceTask), fristKey, output)
			fristKey = kv.Key
			tmpList = append(make([]string, 0), kv.Value)
		}
	}
	if len(tmpList) > 0 {
		output := reduceF(fristKey, tmpList)
		writeOutputFile(mergeName(jobName, reduceTask), fristKey, output)
		fristKey = kv.Key
		tmpList = append(make([]string, 0), kv.Value)
	}
}

func writeOutputFile(filename string, key, content string) {
	f := getFile(filename)
	enc := json.NewEncoder(f)
	enc.Encode(KeyValue{Key:key, Value:content})
	f.Close()
}
func getFile(filename string) (f *os.File) {
	if checkFileIsExist(filename) { //如果文件存在
		f, _ = os.OpenFile(filename, os.O_APPEND|os.O_RDWR, 0777) //打开文件
	} else {
		f, _ = os.Create(filename) //创建文件
	}
	return f
}

func checkFileIsExist(filename string) bool {
	var exist = true
	if _, err := os.Stat(filename); os.IsNotExist(err) {
		exist = false
	}
	return exist
}

type SortKV []KeyValue

func (c SortKV) Less(i, j int) bool {
	a,_ := strconv.Atoi(c[i].Key)
	b,_ := strconv.Atoi(c[i].Value)
	return  a < b
}
func (c SortKV) Len() int {
	return len(c)
}
func (c SortKV) Swap(i, j int) {
	c[i], c[j] = c[j],c[i]
}
