# Spark 

## Spark Architecutre
<ol>
<li>Spark Core : Used for monitoring, scheduling and processing the task</li> 
<li>Spark API (Polyglots) : Supports Java, Scala, Python and R</li> 
<li>Spark SQL & Dataframes: Uses SQL queries. DF supports functional programming to solve data crunching problem</li> 
<li>MLib: Used for Machine Learning</li> 
<li>GraphX: Used for graphs</li> 
<li>Streaming APIs: Streaming: Consume and Process a continuous stream of data</li> 
<li>Cluster manager : Spark runs on Yarn, Mesos, Kubernetes, Standalone & Local</li> 
<li>Distributed storage : HDFS, Amazon S3, NoSQL, RDBMS</li> 
</ol>

![Spark Ecosystem](https://github.com/sumitborhade/Spark/raw/master/Spark_EcoSystem.png)

## Advantages
1. Abstraction : Abrastracts distributed architecture and makes feel like working on single machine and executing SQL queries so underlying complexities are hide.
2. Unified processing: Can use polyglot, provides various liberaries to work for Machine Learning & GraphX
3. Ease to use: Complexities are hiden by Spark
4. Fast processing: In memory processing. 10x faster than Hadoop on disc and 100x faster on memory
5. Powerful caching: It has caching management


## Spark Execution Methods
<ol>
<li>Interactive Clients: Using Spark Shell & Notebook. Used for exploration & development purpose</li>
<li>Spark-Submit (Universely accepted method): Program is submitted using submit job. Used in Production</li>
</ol>

## Spark supports 
1. Batch processing (Scheduled job)
2. Stream processing (Kafka integration)

## Spark Cluster Managers
1. Local
2. Yarn
3. Mesos
4. Kubernetes
5. Standalone

## Spark Processing Model
It works on master slave principles. <br/>
Every application has one master and 0 or more executors. </br>
Work is distributed between the executors, who process and returns the results to master. </br>

![Diagram](https://raw.githubusercontent.com/sumitborhade/Spark/master/Spark_Processing_Model.png)

## Creating Spark Program
Driver has Spark Session (DF/DS) or Spark Context (For RDD).

Spark session is singleton because app has only one driver. 

**Creating Spark Session : Option 1**
```
val spark = SparkSession.builder()
            .appName("My Spark App")
            .master("local[3]")
            .getOrCreate();
spark.stop();
```


**Creating Spark Session : Option 2**
```
val sparkConf = new SparkConf();
sparkConf.set("spark.app.name", "My Spark App");
sparkConf.set("spark.master", "local[3]");

val spark = SparkSession.builder()
            .config(sparkConfig)
            .getOrCreate();
spark.stop();

```

**Creating Spark Session : Option 3**
Create the properties file.
Pro