一个flink程序就是对DataStream的各种转换操作，DataStream API是Flink的核心API，具体有`environment（环境）`->`source（数据源）`->`transformation（转换操作）`->`sink（输出）`和`execute（执行）`五个部分，其主要的编程流程如下：
```java
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(); // env
env  
	.setParallelism(...) // 并行度parallelism
	.socketTextStream(...) // 源算子source  
    .partitionCustom(...) // 分区器partitioner
    .map(...) // 映射算子map 
    .assignTimestampsAndWatermarks( // 分配时间戳和水位线assign watermark  
	    WatermarkStrategy // -配置水位线策略watermark strategy
		    .<Integer>forMonotonousTimestamps()  
		    .withTimestampAssigner(...)  // 自定义时间戳timestamp
            .withIdleness(Duration.ofSeconds(5))) // 允许空闲idleness  
    .keyBy(...) // 按照指定键分区keyby  
    .window(...) // 窗口window
    .allowedLateness(...) // 允许迟到allowLateness
    .sideOutputLateData(...) // 将迟到的数据放入侧输出流
    .process(...) // 处理函数process
    .print(); // 打印算子print  
env.execute(); // 执行execute
```
## 1. 执行环境
创建执行环境，主要是`StreamExecutionEnvironment`类,类中存在如下一些主要的方法：![[flink-env-class.png]]
#### 1.1 getExecutionEnvironment()
创建一个执行环境，可以