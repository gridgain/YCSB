# Running YCSB benchmark for GridGain 9

## 1. Prepare YCSB

1. Clone the GridGain-fork of YCSB

```shell
git clone https://github.com/gridgain/YCSB.git
```

2. Build the project for ignite3 module using Maven

```shell
mvn -pl org.gridgain.ycsb:ignite3-binding -am clean package -Dmaven.test.skip=true
```

The ignite3 module is compatible with GridGain 9.x and Apache Ignite 3.x products.   
**Note**: There is also the gridgain9 module in the repository, which has the clients compatible with GridGain 9.x only.

3. Copy libraries from GridGain distribution /lib folder to the YCSB /dependency folder

```shell
cp -r $GRIDGAIN_DISTRIBUTION/lib $YCSB_REPO/ignite3/target/dependency
```

$GRIDGAIN\_DISTRIBUTION/lib can be a folder in the GridGain DB distribution or the GridGain Java client distribution. Both options will work, but the lighter one is the GridGain Java client distribution.

## 2. Start your GridGain 9 cluster

You need a running GridGain 9 (or Apache Ignite 3) cluster to benchmark against. For setup, you can refer to the [“Getting Started” guide](https://www.gridgain.com/docs/gridgain9/latest/quick-start/getting-started-guide). If [using Docker](https://www.gridgain.com/docs/gridgain9/latest/installation/installing-using-docker), GridGain provides docker images and docker-compose setups that simplify starting a cluster.  
**Note**: The cluster configuration is out of scope of this guide - assume your cluster is up and reachable by the benchmark client.

## 3. Run the benchmark

Once you have built YCSB with ignite3 binding and have a running cluster, you can execute the benchmark.  
The YCSB benchmark performs in two steps (phases): 

1. Load phase, when test data inserts into DB. Usually, we use this phase to measure performance of PUT (INSERT) operations.

```shell
./bin/ycsb.sh load ignite3 -P $WORKLOAD_FILE -p hosts=$CLUSTER_NODES_LIST
```

2. Run phase, when benchmark performs a set of operations from specified distribution. You can specify proportions among CRUD operations. Usually, we use this phase to measure performance of GET (READ) operations only.

```shell
./bin/ycsb.sh run ignite3 -P $WORKLOAD_FILE -p hosts=$CLUSTER_NODES_LIST
```

### Parameters description:

* `load / run`  
  The phase to perform. The load phase prepares the test data. The run phase performs the operations according to the specified distribution.

* `ignite3`  
  Name of the database module to use.

* `-P $WORKLOAD_FILE`  
  File to get properties from. Typically contains a number of properties for a workload.

* `-p {property=value}`  
  Set / override a property with a given value.   
  In our example, `-p hosts=$CLUSTER_NODES_LIST` is used to set the list of cluster nodes.   
  You can also override such properties as `recordcount`, `operationcount`, `threadcount`, etc.  
  See some basic properties description below. Also, you can find more property descriptions on [YCSB Wiki](https://github.com/brianfrankcooper/YCSB/wiki/Core-Properties).

### YCSB properties description:

| Property | Description | Default value |
| :---- | :---- | :---- |
| recordcount | Number of records to insert on load phase or number of records to use on run (transactions) phase. | 1000 |
| operationcount | Number of operations to perform on run (transactions) phase.  | 1000 |
| warmupops | Number of operations to perform as a warmup before the payload. These operations will not be included in the result statistics. | 0 |
| batchsize | Number of processed records within a batch operation if a client supports batches. | 1 |
| threadcount | Number of parallel threads that will perform operations from the client instance. | 1 |
| fieldcount | Number of data fields, excluding the key field. | 10 |
| fieldlength | Size of each data field in bytes. | 100 |

## 4. Interpretation of results

YCSB will output throughput (ops/sec), average latency, and latency percentiles (e.g. p95, p99). This allows you to understand typical latency, tail latency and how many operations per second the cluster handles.  
The YCSB output can look like the following:

```shell
[WARM-UP], StartTime(ms), 1764249199997
[WARM-UP], RunTime(ms), 15866
[WARM-UP], Throughput(ops/sec), 63027.85831337451
[WARM-UP], Operations, 1000000
[PAYLOAD], StartTime(ms), 1764249215863
[PAYLOAD], RunTime(ms), 28669
[PAYLOAD], Throughput(ops/sec), 69761.76357738323
[PAYLOAD], Operations, 2000000
[INSERT], Operations, 2000000
[INSERT], AvgLatency(us), 893.90729
[INSERT], StdDevLatency(us), 2529.3884065905845
[INSERT], MinLatency(us), 142
[INSERT], MaxLatency(us), 247167
[INSERT], 95thPercentileLatency(us), 1303
[INSERT], 99thPercentileLatency(us), 1780
[INSERT], Return=OK, 2000000
```

### Understanding the output structure

A typical YCSB run is divided into distinct parts, each labeled in the output.

* `[WARM-UP]`   
  This part is not included in the final performance statistics. Its purpose is to "warm up" the system: fill caches, allow JIT compilers in the database or JVM to optimize code, establish network connections, and reach a steady operational state. Ignoring warm-up can lead to misleadingly poor initial results.  
* `[PAYLOAD]`  
  This is the main measurement part. All statistics for throughput and latency are derived solely from this period. The operations count here defines the actual workload volume for the benchmark.  
* `[OPERATION TYPE]`  
  The results are then broken down by the type of database operation (e.g., `[INSERT]`, `[READ], [UPDATE], [DELETE]`, etc). You will see a separate block for each operation type defined in your workload file.

Let's look at the provided metrics:

* `[PAYLOAD], Throughput(ops/sec)`  
  The overall throughput for all operation types in the payload period. A higher throughput is generally better, but never view it in isolation. It must be analyzed alongside latency metrics.  
* `AvgLatency(us)`  
  The average latency for an operation, in microseconds. Gives a general sense of performance but can be highly misleading. A single very slow operation (outlier) can skew the average significantly, masking the experience for the majority of requests.  
* `StdDevLatency(us)`  
  A mathematical measure of the latency variance.  
* `MinLatency(us) / MaxLatency(us)`  
  The absolute best- and worst-case latency times observed.  
* `95thPercentileLatency(us) / 99thPercentileLatency(us)`  
  The latency for the 95% / 99% of all operations. Gives understanding of real-world performance. Represents the "typical" performance for most users, excluding the worst 5% (in case of `95thPercentileLatency`) or 1% (in case of `99thPercentileLatency`).

