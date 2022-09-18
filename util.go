package fastmulticache

import (
	"fmt"
	"runtime"
	"sync/atomic"

	"github.com/golang/glog"
)

func BatchResultErr(m map[string]interface{}, err error) {
	var errMsg string
	if err != nil {
		errMsg = err.Error()
	}
	m["errMsg"] = errMsg
}

func BatchGetResultErr(m map[string]interface{}) (err error) {
	errMsg := m["errMsg"].(string)
	if errMsg != "" {
		err = fmt.Errorf(errMsg)
	}
	return
}

func BatchResultVal(m map[string]interface{}, val interface{}) {
	m["val"] = val
}

func BatchGetResultVal(m map[string]interface{}) (val interface{}) {
	val = m["val"]
	return
}

func BatchTaskId(dataMake map[string]interface{}) int64 {
	return dataMake["taskId"].(int64)
}

func BatchDbInsertMakeData(table string, data map[string]interface{}) (dataMake map[string]interface{}) {
	dataMake = make(map[string]interface{})
	dataMake["taskId"] = MakeIdUnique()
	dataMake["table"] = table
	dataMake["data"] = data
	return
}

func BatchDbInsertExtractData(dataMake map[string]interface{}) (taskId int64, table string, data map[string]interface{}) {
	return dataMake["taskId"].(int64), dataMake["table"].(string), dataMake["data"].(map[string]interface{})
}

func BatchKvSetMakeData(key, val string, dataType int, onlySync bool, breakDown bool) (dataMake map[string]interface{}) {
	dataMake = make(map[string]interface{})
	dataMake["taskId"] = MakeIdUnique()
	dataMake["key"] = key
	dataMake["val"] = val
	dataMake["dataType"] = dataType
	dataMake["onlySync"] = onlySync
	dataMake["breakDown"] = breakDown
	return
}

func BatchKvSetExtractData(dataMake map[string]interface{}) (taskId int64, key string, val string, dataType int, onlySync bool, breakDown bool) {
	return dataMake["taskId"].(int64), dataMake["key"].(string), dataMake["val"].(string), dataMake["dataType"].(int), dataMake["onlySync"].(bool), dataMake["breakDown"].(bool)
}

func BatchKvGetMakeData(key string, dataType int) (dataMake map[string]interface{}) {
	dataMake = make(map[string]interface{})
	dataMake["taskId"] = MakeIdUnique()
	dataMake["key"] = key
	dataMake["dataType"] = dataType
	return
}

func BatchKvGetExtractData(dataMake map[string]interface{}) (taskId int64, key string, dataType int) {
	return dataMake["taskId"].(int64), dataMake["key"].(string), dataMake["dataType"].(int)
}

var (
	id int64
)

func MakeIdUnique() int64 {
	return atomic.AddInt64(&id, 1)
}

func SelectQueue() int {
	return int(MakeIdUnique() % int64(_o.queueNum))
}

func SelectDbMu() int {
	return int(MakeIdUnique() % int64(_o.dbMaxParallel))
}

func RecoverCommon() {
	if err := recover(); err != nil {
		buf := make([]byte, 4096)
		runtime.Stack(buf, true)
		glog.Errorf("recover: %v %v", err, string(buf))
	}
}
