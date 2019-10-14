package week3

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.log4j.{ Level, Logger }
import org.apache.spark.rdd.RDD
import org.apache.spark.RangePartitioner
import week3.CFFPurchase
//case class CFFPurchase(customerid: Int, destination: String, price: Double)

object WideVsNarrowDependencies {
  /*
   * Lineages
   * Computations on RDD are represented as a lineage graph; a Directed Acyclic
   * Graph (DAG) representing the computations made on a graph
   * 
   */

  val conf: SparkConf = new SparkConf().setAppName("Spark Pi").setMaster("local[*]")
  val sc: SparkContext = new SparkContext(conf)
  val rootLogger = Logger.getRootLogger()
  rootLogger.setLevel(Level.ERROR)
  def main(args: Array[String]) {
    /*
     * RDDs are made up of 2 important parts (but are made up of 4 parts in
     * total)
     * 
     * 1. Partitions. Atomic pieces of the dataset. One or many per compute 
     * node.
     * 2. Dependencies. Models relationship between this RDD and its
     * partitions with the RDD(s) it was derived from.
     * 3. A function for computing the dataset based on its parent RDDs.
     * 4. Metadata about its partitioning scheme and data placement.
     * 
     * Transformations cause shuffles. Transaformations can have two kind of
     * dependencies...
     * 1. Narrow Dependecies: each partition of the parent RDD is used by at
     *                        at most one partition of the child RDD
     * 2. Wide Dependencies: each partition of the parent RDD may be depended
     *                       on by multiple child partitions... Shuffles
     *                       
     * Transformations with narrow dependencies:
     * - map
     * - mapValues
     * - flatMap
     * - filter
     * - mapPartitions
     * - mapPartitionsWithIndex
     * 
     * Transformations with wide dependencies:
     * - cogroup
     * - groupWith
     * - join
     * - leftOuterJoin
     * - rightOuterJoin
     * - groupByKey
     * - reduceByKey
     * - combineByKey
     * - distinct
     * - intersection
     * - repartition
     * - coalesce
     * 
     * Narrow dependency objects:
     * - OneToOneDependency
     * - PruneDependency
     * - RangeDependency
     * 
     * Wide dependency objects:
     * - ShuffleDependency
     * 
     * toDebugString allowes to know the stages
     * 
     * Lineages graphs are key to fault tolerance in Spark...
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

    val partitioned = pairs.partitionBy(tunedPartitioner).persist()
    
    val purchasesPerCust = partitioned.map( p => (p._1, (1, p._2)))
    
    print(purchasesPerCust.reduceByKey((v1, v2) => (v1._1 + v2._1, v1._2 + v2._2)).toDebugString)
    
    val purchasesPerMonth = purchasesPerCust.reduceByKey((v1, v2) => (v1._1 + v2._1, v1._2 + v2._2)).collect()
    
    //partitioned.reduceByKey((v1, v2) => (v1 ._1 + v2._1, v1 ._2 + v2._2)).toDebugString
    //partitioned.collect().foreach(println)

  }
}