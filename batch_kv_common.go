package fastmulticache

type BatchKvCommon struct {
}

func NewBatchKvCommon() *BatchKvCommon {
	return &BatchKvCommon{}
}

func (b *BatchKvCommon) Get(key string, dataType int) (value interface{}, err error) {
	res := _o.batchKvGet.AddTask(BatchKvGetMakeData(key, dataType))
	err = BatchGetResultErr(res)
	if err == nil {
		value = BatchGetResultVal(res)
	}
	return
}

func (b *BatchKvCommon) Set(key string, value string, dataType int, onlySync bool, breakDown bool) (err error) {
	res := _o.batchKvSet.AddTask(BatchKvSetMakeData(key, value, dataType, onlySync, breakDown))
	err = BatchGetResultErr(res)
	return
}
