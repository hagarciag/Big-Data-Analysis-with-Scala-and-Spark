package week3

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.log4j.{ Level, Logger }
import org.apache.spark.rdd.RDD
import org.apache.spark.RangePartitioner
import week3.CFFPurchase
//case class CFFPurchase(customerid: Int, destination: String, price: Double)

object Partitioning {
  /*
   * Partitions
   * The data within a RDD is split into several partitions.
   *
   * Properties of the partitions:
   * - Partitions never span multiple machines, i.e. tuples in the same
   * partition are guaranteed to be on the same machine.
   * - Each machine in the cluster contains one or more partitions.
   * - The number of partitions to use is configueble. By default, it equals
   * the total number of cores on all executor nodes.
   *
   * Two kinds of partitioning in Spark
   * - Hash partitiong
   * - Range partitionig
   */

  val conf: SparkConf = new SparkConf().setAppName("Spark Pi").setMaster("local[*]")
  val sc: SparkContext = new SparkContext(conf)
  val rootLogger = Logger.getRootLogger()
  rootLogger.setLevel(Level.ERROR)
  def main(args: Array[String]) {
    /*
     * Grouping and Reducing, Example1
     * Goal: calculate how many trips, and how much money was spent by
     * each individual customer over the course of the month.
     *
     * Hash partitioning
     * groupByKey first computes per tuple (k, v) its partition p:
     * p = k.hashCode() % numPartitions
     * Then, all tuples in the same partition p are sent to the machine hosting p.
     * Intuition: hash partitioning attempts to spread data evenly across
     * partitions based on the key.
     *
     * Range partitioning
     * Pair RDDs may contain keys that have an ordering defined.
     * For such RDDs, range partitioning may be more efficient.
     * Using a range partitioner, keys are partitioned according to:
     * 1. an ordering for keys
     * 2. a set of sorted ranges of keys
     *
     * Property: tuples with keys in the same range appear on the same machine.
     *
     * There are two ways to create RDDs with specific partitionings:
     * 1. Call partitionBy on an RDD, providing an explicit Partitioner.
     * 2. Using transformations that return RDDs with specific partitioners.
     * 
     * Creating a RangePartitioner (partitionBy) requires:
     * 1. Specifying the desired number  of partitions
     * 2. Providing a Pair RDD with ordered keys. This RDD is sampled to
     * create a suitable set of sorted ranges.
     * 
     * Partitioner from parent RDD:
     * Pair RDDs that are the result of a transformation on a partitioned
     * Pair RDD typically is configured to use the hash partitioner that was 
     * used to construct it.
     * 
     * Automatically-set partitioners:
     * Some operations on RDDs automatically result in an RDD with a known
     * partitioner - for when it makes sense.
     * For example, by default, when using sortByKey, a RangePartitioner is
     * used. Further, the default partitioner when using groupByKey, is a
     * HashPartitioner, as we saw earler.
     * 
     * Operations on Pair RDDs that hold to ( and propagate) a partitioner:
     * - cogroup
     * - groupWith
     * - join
     * - leftOuterJoin
     * - rightOuterJoin
     * - groupByKey
     * - reduceByKey
     * - foldByKey
     * - combineByKey
     * - partitionBy
     * - sort
     * - mapValues (if parent has a partitioner)
     * - flatMapValues (if parent has a partitioner)
     * - fi1ter (if parent has a partitioner)
     * All other operations will produce a result without a partitioner.
     * 
     * 
     */
    val purchases = List(
      CFFPurchase(100, "Geneva", 22.25),
      CFFPurchase(300, "Zurich", 42.10),
      CFFPurchase(100, "Fribourg", 12.40),
      CFFPurchase(200, "St. Gallen", 8.20),
      CFFPurchase(100, "Lucerne", 31.60),
      CFFPurchase(300, "Basel", 16.20))

    val purchasesRdd = sc.parallelize(purchases)

    val pairs = purchasesRdd.map(p => (p.customerid, p.price))
    val tunedPartitioner = new RangePartitioner(8, pairs)
    //print("\n1\n")
    //print(tunedPartitioner.samplePointsPerPartitionHint)
    //purchasesRdd.mapPartitionsWithIndex( (index: Int, it: Iterator[Int]) =>it.toList.map(x => if (index ==5) {println(x)}).iterator).collect
    //print("\n2\n")
    //print(tunedPartitioner.getPartition(1))
    //print("\n3\n")
    //print(tunedPartitioner.toString())
    val partitioned = pairs.partitionBy(tunedPartitioner).persist()
    
    partitioned.collect().foreach(println)

  }
}