package fastmulticache

type BatchMemGet struct {
}

func NewBatchMemGet() *BatchMemGet {
	return &BatchMemGet{}
}

func (b *BatchMemGet) AddTask(data map[string]interface{}) (res map[string]interface{}) {
	res = make(map[string]interface{})
	_, key, dataType := BatchKvGetExtractData(data)
	vals, _ := _o.mem.GetBatch([]KeyInfo{{Name: key, DataType: dataType}})
	BatchResultVal(res, vals[key])
	BatchResultErr(res, nil)
	return
}

func (b *BatchMemGet) RunAsync() {}
