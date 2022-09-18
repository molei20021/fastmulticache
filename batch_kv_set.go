package fastmulticache

import (
	"sync"
	"time"
)

type BatchKvSet struct {
	queue      []chan map[string]interface{}
	poolChan   sync.Pool
	resChanMap map[int64]chan map[string]interface{}
	mu         sync.Mutex
}

func NewBatchKvSet() (obj *BatchKvSet) {
	obj = &BatchKvSet{
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

func (b *BatchKvSet) AddTask(data map[string]interface{}) (res map[string]interface{}) {
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

func (b *BatchKvSet) RunAsync() {
	for i := 0; i < _o.queueNum; i++ {
		go b.runForever(i)
	}
}

func (b *BatchKvSet) runForever(idx int) {
	var (
		batchExecList = make([]KeyValSetInfo, 0)
		batchTasks    = make([]int64, 0)
		err           error
		batchNum      int
	)
	defer RecoverCommon()
	for {
		select {
		case data := <-b.queue[idx]:
			taskId, key, val, dataType, onlySync, breakDown := BatchKvSetExtractData(data)
			batchExecList = append(batchExecList, KeyValSetInfo{Key: key, Val: val, DataType: dataType, OnlySync: onlySync, BreakDown: breakDown})
			batchTasks = append(batchTasks, taskId)
			batchNum++
			if batchNum >= _o.batchCommitMinNumKv {
				err = _o.kv.SetBatch(batchExecList)
				rspMap := make(map[string]interface{})
				BatchResultErr(rspMap, err)
				for _, taskId := range batchTasks {
					b.mu.Lock()
					chanRes := b.resChanMap[taskId]
					chanRes <- rspMap
					delete(b.resChanMap, taskId)
					b.mu.Unlock()
				}
				batchTasks = make([]int64, 0)
				batchExecList = make([]KeyValSetInfo, 0)
				batchNum = 0
			}
		case <-time.After(time.Millisecond * time.Duration(_o.batchCommitTimeoutMsKv)):
			if batchNum == 0 {
				continue
			}
			err = _o.kv.SetBatch(batchExecList)
			rspMap := make(map[string]interface{})
			BatchResultErr(rspMap, err)
			for _, taskId := range batchTasks {
				b.mu.Lock()
				chanRes := b.resChanMap[taskId]
				chanRes <- rspMap
				delete(b.resChanMap, taskId)
				b.mu.Unlock()
			}
			batchTasks = make([]int64, 0)
			batchExecList = make([]KeyValSetInfo, 0)
			batchNum = 0
		}
	}
}
