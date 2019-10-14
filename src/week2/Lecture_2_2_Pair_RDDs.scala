package week2

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.log4j.{ Level, Logger }

import org.apache.spark.rdd.RDD

object Pair_RDDs {

  val conf: SparkConf = new SparkConf().setAppName("Spark Pi").setMaster("local[*]")
  val sc: SparkContext = new SparkContext(conf)
  val rootLogger = Logger.getRootLogger()
  rootLogger.setLevel(Level.ERROR)
  def main(args: Array[String]) {
    val rdd = sc.parallelize(List("hello", "world", "good", "morning"))
    val pairRdd = rdd.map(a => (a.length, a))
    pairRdd.collect().foreach(println)
    sc.stop()
  }
}
