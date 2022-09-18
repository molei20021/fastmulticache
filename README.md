# fastmulticache

## 介绍

多级缓存引入了一种批量化的查询和更新方法，用户可通过实现SetMem、SetKv等接口接入不同类型的缓存中间件，如memcache、redis等。

### 实现原理
#### kv批量查询

![image](https://github.com/molei20021/fastmulticache/blob/main/resources/kv-read.png)

kv批量查询会将当前收到的所有请求传入查询工作池，查询工作池会将这些请求对应的查询key随机放入n个查询队列中，并通过对象服用池申请n个channel管道来获取查询结果的异步通知。每个查询队列对应一个工作协程，用户可自定义查询队列的属性，如队列长度N，代表队列中的个数达到多少后就放入pipeline执行批量查询；队列数量M，代表并发执行的查询队列的数量有多少，在高QPS时可以设大些以提升系统查询的性能；超时时间T, 代表QPS较低时在T时间内队列中待提交的元素个数<N时，为了维持请求QPS的要求，强制放入pipeline执行批量查询。批量查询后会解析redis批量请求结果，并拆分成n个value并通过n个channel管道来通知查询结果。通过批量查询的方式可以提升redis网络请求带宽的利用率，从而提升整体的QPS。

#### kv批量写入

![image](https://github.com/molei20021/fastmulticache/blob/main/resources/kv-write.png)

kv批量写入会将当前收到的所有请求传入写入工作池，写入工作池会将这些请求对应的写入key及对应的value随机放入n个写入队列中，并通过对象服用池申请n个channel管道来获取写入结果的异步通知。每个写入队列对应一个工作协程，用户可通过环境变量自定义写入队列的属性，如队列长度N，代表队列中的个数达到多少后就放入pipeline执行批量写入；队列数量M，代表并发执行的写入队列的数量有多少，在高QPS时可以设大些以提升系统写入的性能；超时时间T, 代表QPS较低时在T时间内队列中待提交的元素个数<N时，为了维持请求QPS的要求，强制放入pipeline执行批量写入。批量写入后会解析redis批量请求结果，并拆分成n个value并通过n个channel管道来通知写入结果。通过批量写入的方式也可以提升redis网络请求带宽的利用率，从而提升整体的QPS。

#### db批量写入

![image](https://github.com/molei20021/fastmulticache/blob/main/resources/db-write.png)

db批量写入会将当前收到的所有请求传入写入工作池，写入工作池会将这些请求按table名进行分组，并将每组table对应的写入key及对应的value随机放入n个写入队列中，并通过对象服用池申请n个channel管道来获取写入结果的异步通知。每个写入队列对应一个工作协程，用户可通过环境变量自定义写入队列的属性，如队列长度N，代表队列中的个数达到多少后就放入pipeline执行批量写入；队列数量M，代表并发执行的写入队列的数量有多少，在高QPS时可以设大些以提升系统写入的性能；超时时间T, 代表QPS较低时在T时间内队列中待提交的元素个数<N时，为了维持请求QPS的要求，强制放入pipeline执行批量写入。批量写入后会解析mysql批量请求结果，并拆分成n个value并通过n个channel管道来通知写入结果。通过批量写入的方式可以提升mysql网络请求带宽的利用率，从而提升整体的QPS。

### 参数配置方法

| 方法名                    | 含义                             |
| ------------------------- | -------------------------------- |
| SetKv                     | 设置缓存中间件的通用接口         |
| SetMem                    | 设置内存的通用接口               |
| SetExpirationKvSeconds    | redis超时时间(秒)                |
| SetExpirationMemSeconds   | 内存超时时间(秒)                 |
| SetPriorities             | 缓存优先级                       |
| SetBatchCommitTimeoutMsDb | db批量插入的超时时间(毫秒)       |
| SetBatchCommitMinNumDb    | db批量插入最小数量(毫秒)         |
| SetBatchCommitTimeoutMsKv | redis批量执行的超时时间(毫秒)    |
| SetBatchCommitMinNumKv    | redis批量提交的最小数量          |
| SetBufferNum              | 工作池队列的缓冲数               |
| SetQueueNum               | 工作池队列数                     |
| SetDbMaxParallel          | 访问db的最大并行数               |
| SetDbLockTimeoutMs        | 访问db的超时锁的超时时间(毫秒)   |
| SetBreakDownTimeoutMs     | 预防雪崩的kv空数据过期时间(毫秒) |
| SetCacheSync              | 设置缓存跨节点同步通用方法       |
