package fastmulticache

import (
	"sync"
	"time"
)

type BatchKvGet struct {
	queue      []chan map[string]interface{}
	poolChan   sync.Pool
	resChanMap map[int64]chan map[string]interface{}
	mu         sync.Mutex
}

func NewBatchKvGet() (obj *BatchKvGet) {
	obj = &BatchKvGet{
		poolChan: sync.Pool{
			New: func() interface{} { return make(chan map[string]interface{}) },
		},
		resChanMap: make(map[int64]chan map[string]interface{}),
	}
	for i := 0; i < _o.queueNum; i++ {
		obj.queue = append(obj.queue, make(chan map[string]interface{}, _o.bufferNum))
	}
	return
}

func (b *BatchKvGet) AddTask(data map[string]interface{}) (res map[string]interface{}) {
	taskId := BatchTaskId(data)
	chanRes := b.poolChan.Get().((chan map[string]interface{}))
	b.mu.Lock()
	b.resChanMap[taskId] = chanRes
	b.mu.Unlock()
	b.queue[SelectQueue()] <- data
	res = <-chanRes
	b.poolChan.Put(chanRes)
	return res
}

func (b *BatchKvGet) RunAsync() {
	for i := 0; i < _o.queueNum; i++ {
		go b.runForever(i)
	}
}

func (b *BatchKvGet) runForever(idx int) {
	var (
		batchKeys  = make([]KeyInfo, 0)
		batchTasks = make([]int64, 0)
		err        error
		vals       map[string]interface{}
		batchNum   int
	)
	defer RecoverCommon()
	for {
		select {
		case data := <-b.queue[idx]:
			taskId, key, dataType := BatchKvGetExtractData(data)
			batchKeys = append(batchKeys, KeyInfo{Name: key, DataType: dataType})
			batchTasks = append(batchTasks, taskId)
			batchNum++
			if batchNum >= _o.batchCommitMinNumKv {
				vals, err = _o.kv.GetBatch(batchKeys)
				for i, taskId := range batchTasks {
					b.mu.Lock()
					rspMap := make(map[string]interface{})
					BatchResultErr(rspMap, err)
					BatchResultVal(rspMap, vals[batchKeys[i].Name])
					chanRes := b.resChanMap[taskId]
					chanRes <- rspMap
					delete(b.resChanMap, taskId)
					b.mu.Unlock()
				}
				batchTasks = make([]int64, 0)
				batchKeys = make([]KeyInfo, 0)
				batchNum = 0
			}
		case <-time.After(time.Millisecond * time.Duration(_o.batchCommitTimeoutMsKv)):
			if batchNum == 0 {
				continue
			}
			vals, err = _o.kv.GetBatch(batchKeys)
			for i, taskId := range batchTasks {
				b.mu.Lock()
				rspMap := make(map[string]interface{})
				BatchResultErr(rspMap, err)
				BatchResultVal(rspMap, vals[batchKeys[i].Name])
				chanRes := b.resChanMap[taskId]
				chanRes <- rspMap
				delete(b.resChanMap, taskId)
				b.mu.Unlock()
			}
			batchTasks = make([]int64, 0)
			batchKeys = make([]KeyInfo, 0)
			batchNum = 0
		}
	}
}
