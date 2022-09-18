package fastmulticache

type BatchMemCommon struct {
}

func NewBatchMemCommon() *BatchMemCommon {
	return &BatchMemCommon{}
}

func (b *BatchMemCommon) Get(key string, dataType int) (value interface{}, err error) {
	res := _o.batchMemGet.AddTask(BatchKvGetMakeData(key, dataType))
	err = BatchGetResultErr(res)
	if err == nil {
		value = BatchGetResultVal(res)
	}
	return
}

func (b *BatchMemCommon) Set(key string, value string, dataType int, onlySync bool, breakDown bool) (err error) {
	res := _o.batchMemSet.AddTask(BatchKvSetMakeData(key, value, dataType, onlySync, breakDown))
	err = BatchGetResultErr(res)
	return
}
