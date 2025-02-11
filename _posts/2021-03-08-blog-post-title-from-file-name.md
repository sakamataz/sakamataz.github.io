# Understand Spark by asking questions

Some classic Spark questions will be included in this blog. It's suitable for intermediate and senior spark developers.

---

# join optimizations & fault tolerance & AQE

## The definition of Spark wide/narrow dependencies and list op example?

**wide dependencies**

The father RDD's partition relationship with child RDDs is one-to-one (or many-to-one), meaning each partition of the child RDD corresponds to one or more partitions of the father RDD. No shuffle appears in this situation.

ops: join(not hash-partitioned)、groupByKey、partitionBy;

**narrow dependencies**

The father RDD's partition relationship with child RDDs is one-to-many, and exist shuffle.

ops: map、filter、union、join(hash-partitioned)、mapPartitions;

## What join categories does spark have?

shuffle hash join、broad hash join、sort merge join
![image tooltip here](/pics/1.1.png)

## How to deal with data skew in spark?
1. Add repartition violently. (a temporay fix than a real solution. not useful in many situations)
2. Two phase aggragtion. first add random prefix & aggragate, and second remove prefix & aggragate
3. Filter skew keys from data.
4. Convert to map-join, send smaller table by broadcasting. (single suitable for join beween small table to large table)
5. Use AQE. AQE can deal with questions in task level, and reduce data consume in reduce stage.

## How to optimize joins between big tables

1. Split into broadcast hash join. If a big table has partition keys, convert it into a smaller table and use broadcast join.
2. Use shuffle hash join. When data's distribution is 
uniform, we can use partitional shuffle hash join.(use /*+ shuffle_hash(orders) */  as a hint, its ability is to let spark forcibly use SHJ and use the orders table as the build side).
3. Handle Data Skew First – If data skew exists, address it before applying join optimizations.

## What optimizations does AQE have?

1. Adjustment of join strategies. If the filtered data is smaller than broadcast threshold, use the broadcast hash join strategy instead.
2. Automatic partition merging. If data partitions is too small after shuffling, they will be merged.
3. Automatic data-skew processing. AQE will split the skewed data partitions after the reduce stage to decrease the task workloads. (This does not decrease the workloads of shuffle file processing in the map stage).

## What fault tolerance does Spark have?
1. If the stage output fails, the upper DAGScheduler will retry the task.
2. In Spark computation, the task scheduler will retry the task if an inner task fails.
3. Use lineage to re-compute task.
4. Use checkpointing to cache intermediate results.

# data abstraction

## What is RDD?
RDD is an abstraction for distributed data-set. Is is used to store data both in memory and on disk.

Data caching is one of the most important features of RDD abstructions.

## What types of RDD operators are there? How can you create an RDD?

![image tooltip here](/pics/1.2.png)

1. There are two operators in the RDD programming model: transformations operator and action operator. Developers use Transformation operators to define how data is transformed, and the call action operators to execute the computation and either collect the results or materialize them to disk.

2. There are two typical ways to create RDDs in Spark:
 - Use apis like SparkContext.parallelize to create RDD in inner data
 - Use apis like SparkContext.textFile to create RDD in outer data

## When to use map? When to use mapPartitions?
Use map when operating on individual data elements, and use mapPartitions when operating on an entire partition.

In the example blow, it's unnecessary to use map beacuse it create an MD5 object on every execution.

```scala
// convert to Paired RDD from normal RDD
 
import java.security.MessageDigest
 
val cleanWordRDD: RDD[String] = _ 
 
val kvRDD: RDD[(String, Int)] = cleanWordRDD.map{ word =>
  // Get object instance from MD5 object
  val md5 = MessageDigest.getInstance("MD5")
  // calc hash value by MD5
  val hash = md5.digest(word.getBytes).mkString
  // return pair value of hash value and number
  (hash, 1)
}
```
mapPartitions allows data transformation at the partition level.：

```scala
// convert to Paired RDD from normal RDD
 
import java.security.MessageDigest
 
val cleanWordRDD: RDD[String] = _ 
 
val kvRDD: RDD[(String, Int)] = cleanWordRDD.mapPartitions( partition => {
  val md5 = MessageDigest.getInstance("MD5")
  val newPartition = partition.map( word => {
    (md5.digest(word.getBytes()).mkString,1)
  })
  newPartition
})
```

# progress model
## What progress model does Spark have?
![image tooltip here](/pics/1.3.png)

For a complete RDD, each executor is responsible for processing a subset of the data partitions of this RDD. After the task is finished, the executor will communicate and report the task status with driver. Once the driver receives the executors' status, it will split the remaining tasks and distribute them to the executors until the entire computation graph is executed.

## What schedule strategy does Spark hava?
![image tooltip here](/pics/1.4.png)

1. DAGScheduler is responsible for graph construction and partitioning. It constructs the DAG based on user code, the DAG is split into jobs by the action operator, jobs is split to stages based on shuffle status, and stages are then divided into tasks for execution.

2. SchedulerBackend is responsible for resource scheduling and distribution.

3. TaskScheduler is responsible for task distribute. It selects tasks suitable for distributed execution and assigns them to nodes. The task placement strategy includes: progress of Executor, machine locality, and arbitrary address.

![image tooltip here](/pics/1.5.png)

# shuffle

## What is the process of shuffle?

The formation of shuffle files is based on the granularity of map tasks. The Number of map tasks determines the number of shuffle intermediate files, with each map task generating one file.

![image tooltip here](/pics/1.6.png)

The shuffle file contains two files, first one is data file for records (key, value) data pairs, another is an index file for records which reduce task the pair belong to. In other words, the index file marked the relationship between the records in data file and the consumption of the task in the downstream.

Assuming there are N tasks in reduce stage, and them corresponding to N data partitions, the assignment of each record to a specific reduce task during the map phase is determined by following formula:

P = Hash(Record Key) % N

![image tooltip here](/pics/1.7.png)

In the procedure of generating intermediate files, spark will compute, cache and sort data records in data partition using a data structure similar to map. The key of the map structure is （Reduce Task Partition ID，Record Key）, and the value is data value from original data set.

For the data records in data partition, spark calculates the target partition id for each record using the formula metioned earlier, and then insert the key (Reduce Task Partition ID，Record Key) along with the record's value into the Map structure. Once the map structure is filled, Spark sorts the data records in the map by the key, and then spill all the contents to a temporary file on disk.

As the map structure is cleared, Spark can continue to read partition contents of the partition and continue insert data to teh map structure until the Map structure is filled again and overflows again. And so on until all the data records in the data partition are processed.

So far, there are several temporary files in disk, and some data remains in the Map structure in memory. Spark uses the merge sort algorithm to merge all the temporary files and remaining data in the Map structure to generate data files and corresponding index files respectively.The process of generating intermediate files is also called Shuffle Write.

# data aggregation

## What are the stages of data aggregation in spark? which operators does they correspond to?

The full shuffle overhead of groupByKey is significant (sincd it performs aggregation only on the reduce side without map side aggregation). Therefore, reduceByKey, aggregateByKey and combineByKey are generally perferred over groupByKey.

- reduceByKey performs preliminary aggregation on the Map side before writing to disk and shuffling data. In the Map stage, reduceByKey aggregates two data records with the same key (e.g., Streaming), the aggregation logic is defined by function f, which, for example, selects the record with the lager value. This procedure we called it "Map-side aggregation." Accordingly, the computation performed in the Reduce stage after the network shuffle is called "Reduce-side aggregation".The limitation of reduceByKey is that the computation logic in the Reduce stage must be consistent with that in reduceByKey. If a scenario requires different aggregation logic in two phases, reduceByKey is not suitable.

- aggregateByKey allows defining the aggregation logic in map and reduce phase, is a more flexible aggregation operator.

# memory management and storage system

## How is spark's memory management designed?

The shuffle intermediate files consume the disk space of nodes, while the broadcast conditional mainly takes up the memory space of the nodes. RDD Cache, on the other hand, relies on both, consuming both memory and disk.

![image tooltip here](/pics/1.8.png)

Execution Memory is used to execute the distributed tasks. The computations of distrubution tasks include transform, filter, map, sort, aggregate and reduce operations on the data, and all the memory consumption of these computations comes from Execution Memory.

Storage Memory is used to cache distributed data sets, such as RDD Cache and broadcast condition, etc. The RDD Cache refers to the memory duplication that is materialized by RDD. In a longer DAG, if one RDD is quoted multiple times, caching it to memory will greatly improve the performance.

![image tooltip here](/pics/1.9.png)

The storage system is mainly responsible for BlockManager module:

![image tooltip here](/pics/1.10.png)

The core responsibility of BlockManager is to manage the meta data of data blocks which records and maintains address, location, size and status of the data blocks.

![image tooltip here](/pics/1.11.png)

# Cache

![image tooltip here](/pics/1.12.png)

The cache function will actually makes a further call to persist (MEMORY_ONLY) to complete the computation.

# The prepare, redistributuion and persist

![image tooltip here](/pics/1.13.png)

## How to adjust the Parallelism of operators in the pretreatment stage?
Developers can use repartition operators to arbitrarily adjust the parallelism of RDD, while the coalesce operator is only used to decrease the parallelism of RDD.

The repartition operator introduces a shuffle mechansim to scatter, shuffle and evenly distribute data, so it is heavy but ensures uniform distribution.
In comparison, The coalesce operators simply moves data forcibly without an euqal distribution process, which will result in uneven data distribution and data skewing during calculation.

![image tooltip here](/pics/1.14.png)

# Broadcast variables and Accumulators
- The distribute of broadcast variables is not task granularity, but executor granularity, which reduces the overhead of variable distribution.
- The Accumulators are used to store global variables

# Configuration Item

![image tooltip here](/pics/1.15.png)

# Catalyst

The responsibility of Catalyst is to create and optimize the execution plan, it includes three modules, they are create syntax trees and generate execution plans, optimize logical stages, and optimize physical stages.
In the Catalyst optimization stage, Spark SQL converts the user code to AST syntax trees firstly, also known as execution plan, and then adjusts it through logical optimization and physical optimization.
The optimization of the logical stage mainly relies on prior heuristic experience, such as predicate descent and column pruning, to optimize and adjust the execution plan.
The optimization of the physical stage mainly relies on utilizing statistical information to select the optimal execution mechanism or add necessary computing nodes.


# Tungsten

Tungsten is an execution engine that used to connect the Catalyst  execution plan with underlying Spark Core execution engine, it is mainly responsible for optimizing data results and executable code.
Tungsten designed and implemented one binary data structure called Unsafe Row. Unsafe Row is essentially a byte array that stores every data record of a DataFrame by an extremely compact format, significantly reducing storage overhead and improving the store and access effiency.

Unsafe Row imitates the addressing method of the operating system, recording physical addresses and page offsets, and retrieving objects in the heap.