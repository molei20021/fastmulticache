package fastmulticache

var (
	_o *CacheOption
)

type CacheOptionFunc func(*CacheOption) error

func SetDb(db DbOp) CacheOptionFunc {
	return func(option *CacheOption) error {
		option.db = db
		return nil
	}
}

func SetKv(kv CacheOp) CacheOptionFunc {
	return func(option *CacheOption) error {
		option.kv = kv
		return nil
	}
}

func SetMem(mem CacheOp) CacheOptionFunc {
	return func(option *CacheOption) error {
		option.mem = mem
		return nil
	}
}

func SetExpirationKvSeconds(seconds int) CacheOptionFunc {
	return func(option *CacheOption) error {
		option.expirationKvSeconds = seconds
		return nil
	}
}

func SetExpirationMemSeconds(seconds int) CacheOptionFunc {
	return func(option *CacheOption) error {
		option.expirationMemSeconds = seconds
		return nil
	}
}

func SetPriorities(priority ...int) CacheOptionFunc {
	return func(option *CacheOption) error {
		option.priorities = append(option.priorities, priority...)
		return nil
	}
}

func SetBatchCommitTimeoutMsDb(ms int) CacheOptionFunc {
	return func(option *CacheOption) error {
		option.batchCommitTimeoutMsDb = ms
		return nil
	}
}

func SetBatchCommitMinNumDb(minNum int) CacheOptionFunc {
	return func(option *CacheOption) error {
		option.batchCommitMinNumDb = minNum
		return nil
	}
}

func SetBatchCommitTimeoutMsKv(ms int) CacheOptionFunc {
	return func(option *CacheOption) error {
		option.batchCommitTimeoutMsKv = ms
		return nil
	}
}

func SetBatchCommitMinNumKv(minNum int) CacheOptionFunc {
	return func(option *CacheOption) error {
		option.batchCommitMinNumKv = minNum
		return nil
	}
}

func SetBufferNum(num int) CacheOptionFunc {
	return func(option *CacheOption) error {
		option.bufferNum = num
		return nil
	}
}

func SetQueueNum(num int) CacheOptionFunc {
	return func(option *CacheOption) error {
		option.queueNum = num
		return nil
	}
}

func SetCacheSync(sync CacheSync) CacheOptionFunc {
	return func(option *CacheOption) error {
		option.sync = sync
		return nil
	}
}

func SetKeyPrefixSrc(m map[string]string) CacheOptionFunc {
	return func(option *CacheOption) error {
		option.keyPrefixSrcMap = m
		return nil
	}
}

func SetDbMaxParallel(dbMaxParallel int) CacheOptionFunc {
	return func(option *CacheOption) error {
		option.dbMaxParallel = dbMaxParallel
		return nil
	}
}

func SetDbLockTimeoutMs(ms int) CacheOptionFunc {
	return func(option *CacheOption) error {
		option.dbLockTimeoutMs = ms
		return nil
	}
}

func SetBreakDownTimeoutMs(ms int) CacheOptionFunc {
	return func(option *CacheOption) error {
		option.breakDownTimeoutMs = ms
		return nil
	}
}

func Init(options ...CacheOptionFunc) (err error) {
	_o = &CacheOption{
		batchOpCommonMap: make(map[int]BatchCommon),
		keyPrefixSrcMap:  make(map[string]string),
	}
	for _, option := range options {
		if err := option(_o); err != nil {
			return err
		}
	}
	if _o.db == nil {
		err = ErrInitDbOpInvalid
		return
	}
	if _o.kv == nil {
		err = ErrInitKvOpInvalid
		return
	}
	if _o.mem == nil {
		err = ErrInitMemOpInvalid
		return
	}
	if _o.expirationKvSeconds == 0 {
		_o.expirationKvSeconds = DefaultExpirationKvSeconds
	}
	if _o.expirationMemSeconds == 0 {
		_o.expirationMemSeconds = DefaultExpirationMemSeconds
	}

	if len(_o.priorities) == 0 {
		_o.priorities = append(_o.priorities, CacheTypeMemory, CacheTypeKv)
	}

	if _o.batchCommitTimeoutMsDb == 0 {
		_o.batchCommitTimeoutMsDb = DefaultBatchCommitTimeoutMsDb
	}

	if _o.batchCommitMinNumDb == 0 {
		_o.batchCommitMinNumDb = DefaultBatchCommitMinNumDb
	}

	if _o.batchCommitTimeoutMsKv == 0 {
		_o.batchCommitTimeoutMsKv = DefaultBatchCommitTimeoutMsKv
	}

	if _o.batchCommitMinNumKv == 0 {
		_o.batchCommitMinNumKv = DefaultBatchCommitMinNumKv
	}

	if _o.bufferNum == 0 {
		_o.bufferNum = DefaultBufferNum
	}

	if _o.queueNum == 0 {
		_o.queueNum = DefaultQueueNum
	}

	if _o.dbMaxParallel == 0 {
		_o.dbMaxParallel = DefaultDbMaxParallel
	}

	if _o.dbLockTimeoutMs == 0 {
		_o.dbLockTimeoutMs = DefaultDbLockTimeoutMs
	}

	for i := 0; i < _o.dbMaxParallel; i++ {
		_o.dbMutex = append(_o.dbMutex, NewMutexTimeout())
	}

	if _o.sync == nil {
		err = ErrInitCacheSyncInvalid
		return
	}

	go CacheSyncStart()

	_o.batchDbInsert = NewBatchDbInsert()
	_o.batchDbInsert.RunAsync()

	_o.batchKvSet = NewBatchKvSet()
	_o.batchKvSet.RunAsync()

	_o.batchKvGet = NewBatchKvGet()
	_o.batchKvGet.RunAsync()

	_o.batchMemSet = NewBatchMemSet()
	_o.batchKvSet.RunAsync()

	_o.batchMemGet = NewBatchMemGet()
	_o.batchKvGet.RunAsync()

	for _, u := range _o.priorities {
		if u == CacheTypeMemory {
			_o.batchOpCommonMap[u] = NewBatchMemCommon()
		} else if u == CacheTypeKv {
			_o.batchOpCommonMap[u] = NewBatchKvCommon()
		} else {
			err = ErrPriorityInvalid
			return
		}
	}

	return nil
}
