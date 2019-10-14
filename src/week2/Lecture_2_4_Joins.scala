package week2

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.log4j.{ Level, Logger }
import org.apache.spark.rdd.RDD

object Joins {
  /*
   * Joins are transformations
   * - Inner joins (join)
   * - Outer joins (leftOuterJoin/rightOuterJoin)
   */

  val conf: SparkConf = new SparkConf().setAppName("Spark Pi").setMaster("local[*]")
  val sc: SparkContext = new SparkContext(conf)
  val rootLogger = Logger.getRootLogger()
  rootLogger.setLevel(Level.ERROR)
  def main(args: Array[String]) {
    /*
     * join
     * This operation produces a RDD with values of both RDD whose keys coincide.
     * It yields a RDD[K, (V, W)]
     */
    val as = List((101, ("Ruetli", "AG")), (102, ("Brelaz", "DemiTarif")),
      (103, ("Gress", "Demi Tari fVi sa")), (104, ("Sc hat ten", "Demi Tari f")))
    val abos = sc.parallelize(as)
    val ls = List((101, "Bern"), (101, "Thun"), (102, "Lausanne"), (102, "Geneve"),
      (102, "Nyon"), (103, "Zurich"), (103, "St-Gallen"), (103, "Chur"))
    val locations = sc.parallelize(ls)
    
    val trackedCustomers = abos.join(locations)
    
    print ("\n\njoin:\n")
    print(trackedCustomers.collect().foreach(println))

    /*
     * leftOuterJoin
     * This operation produces a RDD with the elements of the left and
     * the elements of right whose keys coincide with any element of the
     * left.
     * It yields a RDD[(K, (V, Option[W]))
     */    
    val abosWithOptionallocations = abos.leftOuterJoin(locations)
    
    print ("\n\nleftOuterJoin:\n")
    print(abosWithOptionallocations.collect().foreach(println))

    /*
     * rightOuterJoin
     * This operation produces a RDD with the elements of the right and
     * the elements of the left whose keys coincide with any element of the
     * right.
     * It yields a RDD[(K, (Option[V], W))
     */    
    val customersWithlocationDataAndOptionalAbos = abos.rightOuterJoin(locations)
    
    print ("\n\nrightOuterJoin:\n")
    print(customersWithlocationDataAndOptionalAbos.collect().foreach(println))

  }
}