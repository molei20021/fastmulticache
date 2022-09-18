package fastmulticache

import (
	"encoding/json"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/gogf/gf/container/gset"
	"github.com/gogf/gf/text/gstr"
	"github.com/gogf/gf/util/gconv"
	"github.com/golang/glog"
)

var (
	ErrInitKvOpInvalid      = fmt.Errorf("init kv op invalid")
	ErrInitDbOpInvalid      = fmt.Errorf("init db op invalid")
	ErrInitMemOpInvalid     = fmt.Errorf("init mem op invalid")
	ErrPriorityInvalid      = fmt.Errorf("priority invalid")
	ErrNotExist             = fmt.Errorf("not exist")
	ErrInitCacheSyncInvalid = fmt.Errorf("init cache sync invalid")
)

const (
	DefaultExpirationKvSeconds    = 3600
	DefaultExpirationMemSeconds   = 3600
	DefaultBatchCommitTimeoutMsDb = 10
	DefaultBatchCommitMinNumDb    = 10
	DefaultBatchCommitTimeoutMsKv = 10
	DefaultBatchCommitMinNumKv    = 10
	DefaultBufferNum              = 1000
	DefaultQueueNum               = 10
	DefaultDbMaxParallel          = 15
	DefaultDbLockTimeoutMs        = 50
)

const (
	CacheTypeMemory = 1
	CacheTypeKv     = 2
)

var (
	CacheTypeNameMap = map[int]string{
		CacheTypeMemory: "memory",
		CacheTypeKv:     "kv",
	}
)

const (
	DataTypeSingle = 1
	DataTypeList   = 2
)

const (
	PublishMemChannel = "pub_mem"
)

var (
	CacheTypePriMap = map[string][]int{
		"local":  {CacheTypeMemory},
		"remote": {CacheTypeKv},
		"all":    {CacheTypeMemory, CacheTypeKv},
	}
)

type CacheOp interface {
	GetBatch(keys []KeyInfo) (values map[string]interface{}, err error)
	SetBatch(data []KeyValSetInfo) (err error)
	GetKeys(maxNum int) ([]KeyInfo, error)
	DelKey(keyInfo KeyInfo) error
}

type DbOp interface {
	InsertBatch(table string, data []map[string]interface{}) (err error)
	Get(table string, order string, query interface{}, args ...interface{}) (value []map[string]interface{}, err error)
	Update(table string, data map[string]interface{}, query interface{}, args ...interface{}) (err error)
	GetPage(table string, offset int, limit int) ([]map[string]interface{}, error)
}

type KeyInfo struct {
	Name     string
	DataType int
}

type KeyValSetInfo struct {
	Key       string
	Val       string
	DataType  int
	OnlySync  bool
	BreakDown bool
}

type CacheSync interface {
	Subscribe() (chan KeyValSetInfo, error)
}

type CacheOption struct {
	db                     DbOp
	kv                     CacheOp
	mem                    CacheOp
	expirationKvSeconds    int
	expirationMemSeconds   int
	priorities             []int
	batchKvSet             Batch
	batchKvGet             Batch
	batchMemSet            Batch
	batchMemGet            Batch
	batchDbInsert          Batch
	batchCommitTimeoutMsDb int
	batchCommitMinNumDb    int
	batchCommitTimeoutMsKv int
	batchCommitMinNumKv    int
	batchOpCommonMap       map[int]BatchCommon
	bufferNum              int
	queueNum               int
	sync                   CacheSync
	keyPrefixSrcMap        map[string]string
	dbMaxParallel          int
	dbMutex                []*MutexTimeout
	dbLockTimeoutMs        int
	breakDownTimeoutMs     int // 缓存击穿超时
}

type CacheStat struct {
	CacheNumLocal     int64
	CacheNumRemote    int64
	LastRateLocal     float64
	LastRateRemote    float64
	RequestsNumLocal  int64
	RequestsNumRemote int64
}

var (
	stat = new(CacheStat)
)

func (s *CacheStat) AddStat(useCache bool, cacheType int) {
	atomic.AddInt64(&(s.RequestsNumLocal), 1)
	atomic.AddInt64(&(s.RequestsNumRemote), 1)
	if useCache {
		if cacheType == CacheTypeKv {
			atomic.AddInt64(&(s.CacheNumRemote), 1)
		} else {
			atomic.AddInt64(&(s.CacheNumLocal), 1)
		}
	}
	if s.RequestsNumLocal >= 1000 {
		s.LastRateLocal = float64(s.CacheNumLocal) / float64(s.RequestsNumLocal)
		s.LastRateRemote = float64(s.CacheNumRemote) / float64(s.RequestsNumRemote)
		atomic.StoreInt64(&(s.RequestsNumLocal), 0)
		atomic.StoreInt64(&(s.CacheNumLocal), 0)
		atomic.StoreInt64(&(s.RequestsNumRemote), 0)
		atomic.StoreInt64(&(s.CacheNumRemote), 0)
	}
}

func (s *CacheStat) RateLocal() float64 {
	if s.LastRateLocal != 0 {
		return s.LastRateLocal
	} else if s.RequestsNumLocal != 0 {
		return float64(s.CacheNumLocal) / float64(s.RequestsNumLocal)
	}
	return 0
}

func (s *CacheStat) RateRemote() float64 {
	if s.LastRateRemote != 0 {
		return s.LastRateRemote
	} else if s.RequestsNumRemote != 0 {
		return float64(s.CacheNumRemote) / float64(s.RequestsNumRemote)
	}
	return 0
}

func (s *CacheStat) ResetLocal() {
	atomic.StoreInt64(&(s.CacheNumLocal), 0)
	atomic.StoreInt64(&(s.RequestsNumLocal), 0)
	s.LastRateLocal = 0
}

func (s *CacheStat) ResetRemote() {
	atomic.StoreInt64(&(s.CacheNumRemote), 0)
	atomic.StoreInt64(&(s.RequestsNumRemote), 0)
	s.LastRateRemote = 0
}

func CacheDbGet(cacheType string, dataType int, table string, key string, order string, query interface{}, args ...interface{}) (value map[string]interface{}, err error) {
	var val interface{}
	value = make(map[string]interface{})
	var valueData []map[string]interface{}
	for _, u := range getPriorities(cacheType) {
		if val, err = _o.batchOpCommonMap[u].Get(key, dataType); err != nil {
			continue
		}
		if val == "" || val == nil {
			continue
		}
		if dataType == DataTypeSingle {
			valueData = []map[string]interface{}{gconv.Map(val)}
		} else {
			valueData = gconv.Maps(val)
		}
		value["data"] = valueData
		value["from"] = CacheTypeNameMap[u]
		stat.AddStat(true, u)
		if cacheType == "all" && u == CacheTypeKv {
			for i := len(valueData) - 1; i >= 0; i-- {
				dataByte, _ := json.Marshal(valueData[i])
				_o.batchOpCommonMap[CacheTypeKv].Set(key, ByteToStr(dataByte), dataType, true, false)
			}
		}
		return
	}
	stat.AddStat(false, 0)
	value["from"] = "mysql"
	idx := SelectDbMu()
	if _o.dbMutex[idx].LockTimeout(time.Millisecond * time.Duration(_o.dbLockTimeoutMs)) {
		valueData, err = _o.db.Get(table, order, query, args...)
		_o.dbMutex[idx].Unlock()
	} else {
		value["data"] = nil
		return
	}
	var breakDown bool
	if len(valueData) == 0 {
		breakDown = true
		valueData = append(valueData, map[string]interface{}{})
	}
	value["data"] = valueData
	var onlySync bool
	if cacheType == "local" {
		onlySync = true
	}
	for i := len(valueData) - 1; i >= 0; i-- {
		dataByte, _ := json.Marshal(valueData[i])
		_o.batchOpCommonMap[CacheTypeKv].Set(key, ByteToStr(dataByte), dataType, onlySync, breakDown)
	}
	return
}

func CacheGet(cacheType string, dataType int, table string, key string, order string, query interface{}, args ...interface{}) (value map[string]interface{}, err error) {
	var val interface{}
	value = make(map[string]interface{})
	var valueData []map[string]interface{}
	for _, u := range getPriorities(cacheType) {
		if val, err = _o.batchOpCommonMap[u].Get(key, dataType); err != nil {
			glog.Error(err)
			continue
		}
		if val == "" || val == nil {
			continue
		}
		if dataType == DataTypeSingle {
			valueData = []map[string]interface{}{gconv.Map(val)}
		} else {
			valueData = gconv.Maps(val)
		}
		value["data"] = valueData
		value["from"] = CacheTypeNameMap[u]
		stat.AddStat(true, u)
		if cacheType == "all" && u == CacheTypeKv {
			for i := len(valueData) - 1; i >= 0; i-- {
				dataByte, _ := json.Marshal(valueData[i])
				_o.batchOpCommonMap[CacheTypeKv].Set(key, ByteToStr(dataByte), dataType, true, false)
			}
		}
		return
	}
	return
}

func CacheRefreshForce(cacheType string, dataType int, table string, key string, query interface{}, args ...interface{}) {
	var (
		err  error
		data []map[string]interface{}
	)
	idx := SelectDbMu()
	if _o.dbMutex[idx].LockTimeout(time.Millisecond * time.Duration(_o.dbLockTimeoutMs)) {
		data, err = _o.db.Get(table, "", query, args...)
		if err != nil {
			glog.Error(err)
		}
		_o.dbMutex[idx].Unlock()
	} else {
		return
	}

	if len(data) == 0 {
		data = append(data, map[string]interface{}{})
	}
	for _, u := range data {
		dataByte, _ := json.Marshal(u)
		_o.batchOpCommonMap[CacheTypeKv].Set(key, ByteToStr(dataByte), dataType, false, false)
	}
}

func CacheDbInsert(cacheType string, dataType int, table string, data map[string]interface{}, key string) (err error) {
	res := _o.batchDbInsert.AddTask(BatchDbInsertMakeData(table, data))
	if err = BatchGetResultErr(res); err != nil {
		glog.Error(err)
		return
	}
	dataByte, _ := json.Marshal(data)
	_o.batchOpCommonMap[CacheTypeKv].Set(key, ByteToStr(dataByte), dataType, false, false)
	return
}

func needUpdate(table string, data map[string]interface{}, key string, query interface{}, args ...interface{}) bool {
	val, err := CacheGet("all", DataTypeSingle, table, key, "", query, args)
	if err != nil {
		glog.Error(err)
		return true
	}
	dataOld := val["data"]
	if dataOld == nil {
		return true
	}
	dataOldObjs := dataOld.([]map[string]interface{})
	if len(dataOldObjs) == 0 {
		return true
	}
	dataOldObj := dataOldObjs[0]
	dataOldObjByte, _ := json.Marshal(dataOldObj)
	dataNewObjByte, _ := json.Marshal(data)
	return ByteToStr(dataOldObjByte) != ByteToStr(dataNewObjByte)
}

func CacheDbUpdate(cacheType string, table string, data map[string]interface{}, key string, query interface{}, args ...interface{}) (err error) {
	if !needUpdate(table, data, key, query, args...) {
		return
	}
	idx := SelectDbMu()
	if _o.dbMutex[idx].LockTimeout(time.Millisecond * time.Duration(_o.dbLockTimeoutMs)) {
		err = _o.db.Update(table, data, query, args...)
		_o.dbMutex[idx].Unlock()
	} else {
		return
	}
	if err != nil {
		glog.Error(err)
		return
	}
	dataByte, _ := json.Marshal(data)
	err = _o.batchOpCommonMap[CacheTypeKv].Set(key, ByteToStr(dataByte), DataTypeSingle, false, false)
	return
}

func CacheTimeout(breakDown bool, cacheType int) time.Duration {
	if breakDown {
		return time.Millisecond * time.Duration(_o.breakDownTimeoutMs)
	} else if cacheType == CacheTypeKv {
		return time.Second * time.Duration(_o.expirationKvSeconds)
	} else {
		return time.Second * time.Duration(_o.expirationMemSeconds)
	}
}

func getPriorities(cacheType string) []int {
	if p, ok := CacheTypePriMap[cacheType]; ok {
		return p
	}
	return _o.priorities
}

func CacheLocalRate() float64 {
	return stat.RateLocal()
}

func CacheRemoteRate() float64 {
	return stat.RateRemote()
}

func CacheResetLocalRate() {
	stat.ResetLocal()
}

func CacheResetRemoteRate() {
	stat.ResetRemote()
}

func CacheSyncStart() {
	defer RecoverCommon()
	msgChan, err := _o.sync.Subscribe()
	if err != nil {
		glog.Error(err)
		return
	}
	for msg := range msgChan {
		_o.batchOpCommonMap[CacheTypeMemory].Set(msg.Key, msg.Val, msg.DataType, false, msg.BreakDown)
	}
}

func CacheMakeKey(keyName string, value interface{}) string {
	return fmt.Sprintf("%v:%v", keyName, value)
}

func CacheExtractKey(key string) (keyPrefix string, keySuffix string) {
	a1 := gstr.Explode(":", key)
	if len(a1) == 2 {
		keyPrefix = a1[0]
		keySuffix = a1[1]
	}
	return
}

func CacheKeyInfo(key string) (src string, keyVal string, keyPrefix string) {
	keyPrefix, keyVal = CacheExtractKey(key)
	src = _o.keyPrefixSrcMap[keyPrefix]
	return
}

// 缓存预热
func CacheDbPreload(tableName string, keyName string, dataType int, maxNum int) {
	glog.Infof("start CacheDbPreload tableName:%v keyName:%v dataType:%v maxNum:%v", tableName, keyName, dataType, maxNum)
	var offset = 0
	var values []map[string]interface{}
	var err error
	var processedSet = gset.NewStrSet()
	if maxNum <= 0 {
		return
	}
	for {
		values, err = _o.db.GetPage(tableName, offset, 100)
		if err != nil {
			glog.Error(err)
			break
		}
		if len(values) == 0 {
			break
		}

		for _, value := range values {
			valStr := gconv.String(value[keyName])
			if processedSet.Contains(valStr) {
				continue
			}
			keyInfo := KeyInfo{Name: CacheMakeKey(keyName, value[keyName]), DataType: dataType}
			_o.kv.DelKey(keyInfo)
			_o.mem.DelKey(keyInfo)
			CacheRefreshForce("all", dataType, tableName, keyInfo.Name, fmt.Sprintf("%v = ?", keyName), value[keyName])
			processedSet.Add(valStr)
		}

		offset += 100
		if offset > maxNum {
			break
		}
	}
}

// 缓存重建
func CacheRebuild(maxNum int) {
	var (
		err  error
		keys []KeyInfo
	)
	if keys, err = _o.mem.GetKeys(maxNum); err != nil {
		glog.Error(err)
		return
	}
	for _, keyInfo := range keys {
		tableName, keyVal, keyPrefix := CacheKeyInfo(keyInfo.Name)
		_o.kv.DelKey(keyInfo)
		_o.mem.DelKey(keyInfo)
		CacheRefreshForce("all", keyInfo.DataType, tableName, CacheMakeKey(keyPrefix, keyVal), fmt.Sprintf("%v = ?", keyPrefix), keyVal)
	}
}
