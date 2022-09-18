package fastmulticache

import (
	"sync"
	"time"
)

type BatchDbInsert struct {
	queue      []chan map[string]interface{}
	poolChan   sync.Pool
	resChanMap map[int64]chan map[string]interface{}
	mu         sync.Mutex
}

func NewBatchDbInsert() (obj *BatchDbInsert) {
	obj = &BatchDbInsert{
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

func (b *BatchDbInsert) AddTask(data map[string]interface{}) (res map[string]interface{}) {
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

func (b *BatchDbInsert) RunAsync() {
	for i := 0; i < _o.queueNum; i++ {
		go b.runForever(i)
	}
}
func (b *BatchDbInsert) runForever(idx int) {
	var (
		batchExecMap = make(map[string]interface{})
		batchTaskMap = make(map[string][]int64)
		err          error
		batchNum     int
	)
	defer RecoverCommon()
	for {
		select {
		case data := <-b.queue[idx]:
			taskId, table, dataEx := BatchDbInsertExtractData(data)
			if _, ok := batchExecMap[table]; !ok {
				batchExecMap[table] = make([]map[string]interface{}, 0)
			}
			v := batchExecMap[table].([]map[string]interface{})
			v = append(v, dataEx)
			batchExecMap[table] = v
			batchTaskMap[table] = append(batchTaskMap[table], taskId)
			batchNum++
			if batchNum >= _o.batchCommitMinNumDb {
				for k, v := range batchExecMap {
					err = _o.db.InsertBatch(k, v.([]map[string]interface{}))
					rspMap := make(map[string]interface{})
					BatchResultErr(rspMap, err)
					for _, taskId := range batchTaskMap[k] {
						b.mu.Lock()
						chanRes := b.resChanMap[taskId]
						chanRes <- rspMap
						delete(b.resChanMap, taskId)
						b.mu.Unlock()
					}
					delete(batchTaskMap, k)
				}

				batchExecMap = make(map[string]interface{})
				batchNum = 0
			}
		case <-time.After(time.Millisecond * time.Duration(_o.batchCommitTimeoutMsDb)):
			if batchNum == 0 {
				continue
			}
			for k, v := range batchExecMap {
				err = _o.db.InsertBatch(k, v.([]map[string]interface{}))
				rspMap := make(map[string]interface{})
				BatchResultErr(rspMap, err)
				for _, taskId := range batchTaskMap[k] {
					b.mu.Lock()
					chanRes := b.resChanMap[taskId]
					chanRes <- rspMap
					delete(b.resChanMap, taskId)
					b.mu.Unlock()
				}
				delete(batchTaskMap, k)
			}

			batchExecMap = make(map[string]interface{})
			batchNum = 0
		}
	}
}
