package fastmulticache

type Batch interface {
	AddTask(data map[string]interface{}) map[string]interface{}
	RunAsync()
}

type BatchCommon interface {
	Get(key string, dataType int) (value interface{}, err error)
	Set(key string, value string, dataType int, onlySync bool, breakDown bool) (err error)
}
