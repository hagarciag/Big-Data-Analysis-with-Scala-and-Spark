package week2

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.log4j.{ Level, Logger }
import org.apache.spark.rdd.RDD

object Transformations_Actions_Pair_RDDs {
  /*
   * Important operations defined on Pair
   * RDDs (but not available on regular RDDs):
   * Transformations
   * - groupByKey
   * - reduceByKey
   * - mapValues
   * - keys
   * - join
   * - leftOuterJoin/rightOuterJoin
   *
   * Actions
   * - countByKey
   */

  val conf: SparkConf = new SparkConf().setAppName("Spark Pi").setMaster("local[*]")
  val sc: SparkContext = new SparkContext(conf)
  val rootLogger = Logger.getRootLogger()
  rootLogger.setLevel(Level.ERROR)
  def main(args: Array[String]) {
    /*
     * groupBy
     * In Scala the group by creates a map according the function provided.
     * It yields a Map[key, traversable[A]]
     */
    val ages = List(2, 52, 44, 23, 17, 14, 12, 82, 51, 64)
    val grouped = ages.groupBy { age =>
      if (age >= 18 && age < 65) "adult"
      else if (age < 18) "child"
      else "senior"
    }
    print("\n\ngroupBy:\n")
    print(grouped)
    
    /*
     * groupByKey
     * In Spark the group by key does not need a function because it
     * alreary knows it needs to group together the elements with the
     * same key.
     * It yields a RDD[(key, iterable[V])]
     */
    val rdd = sc.parallelize(List("hello", "world", "good", "morning"))
    val pairRdd = rdd.map(a => (a.length, a))
    val groupPairKey = pairRdd.groupByKey()
    print("\n\ngroupByKey:\n")
    groupPairKey.collect().foreach(println)
    
    /*
     * reduceByKey
     * In Spark this transformation can be thought od as a combination
     * of groupByKey and reducing on all the values per key. It's more
     * efficient though, than using each separately. (We'll see why later.)
     * It yields RDD[(K, V)]
     */
    val reducePairKey = pairRdd.reduceByKey((total,value)=> total + value)
    print("\n\nreduceByKey:\n")
    reducePairKey.collect().foreach(println)

    /*
     * mapValues
     * In Spark this transformation simply applies a function to only
     * the values part in a pair RDD. So, it is not able to affect the key
     * and is not applicable to regular RDD, only to Pair RDDs.
     * It differs from map because the latter can be applied to regular
     * RDDs and can be used to alter the key
     */
    val mapValues = pairRdd.mapValues(values => (values, 1))
    print("\n\nmapValues:\n")
    mapValues.collect().foreach(println)
    
    /*
     * keys
     * In Spark this transformation return a RDD with the keys of each tuple.
     * It yields a RDD: RDD[K]
     */
    val keys = pairRdd.keys
    print("\n\nkeys:\n")
    keys.collect().foreach(println)
  }
}