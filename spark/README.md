#### 从大方向来说，Spark 算子大致可以分为以下两类

1. Transformation 变换/转换算子：这种变换并不触发提交作业，完成作业中间过程处理。

   Transformation 操作是延迟计算的，也就是说从一个RDD 转换生成另一个 RDD 的转换操作不是马上执行，需要等到有 Action 操作的时候才会真正触发运算。

2. Action 行动算子：这类算子会触发 SparkContext 提交 Job 作业。

   Action 算子会触发 Spark 提交作业（Job），并将数据输出 Spark系统。



#### 从小方向来说，Spark 算子大致可以分为以下三类:

1. Value数据类型的Transformation算子，这种变换并不触发提交作业，针对处理的数据项是Value型的数据。
2. Key-Value数据类型的Transfromation算子，这种变换并不触发提交作业，针对处理的数据项是Key-Value型的数据对。
3. Action算子，这类算子会触发SparkContext提交Job作业。

##### 1 Value数据类型的Transformation算子

1. Value数据类型的Transformation算子
   - map
   - flatMap
   - mapPartitions
   - glom
2. 输入分区与输出分区多对一型
   - union
   - cartesian
3. 输入分区与输出分区多对多型
   - grouBy
4. 输出分区为输入分区子集型
   - filter
   - distinct
   - subtract
   - sample
   - takeSample
5. Cache型
   - cache
   - persist

##### 2 Key-Value数据类型的Transfromation算子

1. 输入分区与输出分区一对一
   - mapValues
2. 对单个RDD或两个RDD聚集
   - 单个RDD聚集
     - combineByKey
     - reduceByKey
     - partitionBy
   - 两个RDD聚集
     - Cogroup
3. 连接
   - join
   - leftOutJoin
   - rightOutJoin



#####  3 Action算子

1. 无输出
   - foreach
2. HDFS
   - saveAsTextFile
   - saveAsObjectFile
3. Scala集合和数据类型
   - collect
   - collectAsMap
   - reduceByKeyLocally
   - lookup
   - count
   - top
   - reduce
   - fold
   - aggregate