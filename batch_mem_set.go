package fastmulticache

type BatchMemSet struct {
}

func NewBatchMemSet() *BatchMemSet {
	return &BatchMemSet{}
}

func (b *BatchMemSet) AddTask(data map[string]interface{}) (res map[string]interface{}) {
	res = make(map[string]interface{})
	_, key, val, dataType, _, breakDown := BatchKvSetExtractData(data)
	err := _o.mem.SetBatch([]KeyValSetInfo{{Key: key, Val: val, DataType: dataType, BreakDown: breakDown}})
	BatchResultErr(res, err)
	return
}

func (b *BatchMemSet) RunAsync() {}
