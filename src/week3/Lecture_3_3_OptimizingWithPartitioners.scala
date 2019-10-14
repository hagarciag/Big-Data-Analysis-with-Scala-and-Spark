package week3

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.log4j.{ Level, Logger }
import org.apache.spark.rdd.RDD
import org.apache.spark.RangePartitioner
import week3.CFFPurchase
//case class CFFPurchase(customerid: Int, destination: String, price: Double)

object Lecture_3_3_OptimizingWithPartitioners {
  /*
   * How do I know a shuffle will occur?
   * Rule of thumb: a shuffle can occur when the resulting RDD depends on others
   * elements from the same RDD or another RDD.
   */

  val conf: SparkConf = new SparkConf().setAppName("Spark Pi").setMaster("local[*]")
  val sc: SparkContext = new SparkContext(conf)
  val rootLogger = Logger.getRootLogger()
  rootLogger.setLevel(Level.ERROR)
  def main(args: Array[String]) {
    /*
     * We saw in the last session that Spark makes a few kinds of partitioners
     * available out-of-the-box to users:
     * - hash partitioners and
     * - range partitioners.
     * 
     * We also learned what kinds of operations may introduce new partitioners,
     * or which may discard custom partitioners. However, we haven't covered
     * why someone would want to repartition their data.
     * 
     * Using range partitioners we can optimize our earlier use of reduceByKey so
     * that it does not involve any shuffling over the network at all!
     * 
     * 
     * Operations that might cause shuffle:
     * - cogroup
     * - groupWith
     * - join
     * - leftOuter Join
     * - rightOuterJoin
     * - groupByKey
     * - reduceByKey
     * - combineByKey
     * - distinct
     * - intersection
     * - repartition
     * - coalesce
     * 
     * There are a few ways to use operations that might cause a shuffle and to
     * still avoid much or all network shuffling...
     * 
     * 1. reduceByKey running on a pre-partitioned ROD will cause the values
     * to be computed locally, requiring only the final reduced value has to
     * be sent from the worker to the driver.
     * 2. join called on two RDDs that are pre-partitioned with the same
     * partitioner and cached on the same machine will cause the join to be
     * computed locally, with no shuffling across the network.
     * 
     * How your data is organized on the cluster, and what operations
     * you're doing with it matters!
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