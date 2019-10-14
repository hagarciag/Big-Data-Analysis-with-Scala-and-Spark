package week3

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.log4j.{ Level, Logger }
import org.apache.spark.rdd.RDD

case class CFFPurchase(customerid: Int, destination: String, price: Double)

object Shuffling {
  /*
   * Shuffling
   * This refers to the action of moving data from one node to another.
   * When using groupByKey they need we typically have to move data to be
   * "grouped with" its key. Doing this is called "shuffling".
   * This is a huge problem: Latency!
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
     */
    val purchases = List(
      CFFPurchase(100, "Geneva", 22.25),
      CFFPurchase(300, "Zurich", 42.10),
      CFFPurchase(100, "Fribourg", 12.40),
      CFFPurchase(200, "St. Gallen", 8.20),
      CFFPurchase(100, "Lucerne", 31.60),
      CFFPurchase(300, "Basel", 16.20))

      val purchasesRdd = sc.parallelize(purchases)
    // Returns: Array[(Int, (Int, Double))]
    val purchasesPerMonth =
      purchasesRdd.map(p => (p.customerid, p.price)) // Pair RDD
        .groupByKey() // groupByKey returns RDD[(K, Iterable[VJ )J
        .map(p => (p._1, (p._2.size, p._2.sum)))
        .collect()
    print("\n\nGrouping and Reducing - groupBy:\n")
    print(purchasesPerMonth.foreach(println))

    /*
     * Grouping and Reducing, Example2
     * Goal: calculate how many trips, and how much money was spent by
     * each individual customer over the course of the month.
     * reduceByKey reduces in the map phase before the shuffle, and it is done
     * after the shuffle
     */
    // Returns: Array[(Int, (Int, Double))]
    val purchasesPerMonth2 =
      purchasesRdd.map(p => (p.customerid, (1, p.price))) // Pair RDD
        .reduceByKey((v1, v2) => (v1 ._1 + v2._1, v1 ._2 + v2._2))

        .collect()
    print("\n\nGrouping and Reducing - reduceByKey:\n")
    print(purchasesPerMonth2.foreach(println))


  }
}