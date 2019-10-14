package week4

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.log4j.{ Level, Logger }
import org.apache.spark.rdd.RDD


object StructuredVsUnstructuredData {
  /*
   * StructuredVsUnstructuredData
   * 
   * Unstructured: log files, images, etc
   * Semi-structured (self-defining): JSON, XMLs
   * Structured: Database tables
   * 
   * Spark + regular RDDs do not know anything about the schema of the data
   * it's dealing with.
   * 
   * Spark can't see inside objects of RDDs of objects or analize how it may
   * be used, and to optimize based on that usage. It's opaque.
   * 
   * The same can be said about computation.
   * In Spark RDDs:
   * - We do functional transformations on data.
   * - We pass used-defined function literals to high-order functions like
   *   map, flatMap, and filter.
   *   
   * Like the data Spark operates on with RDDS, function literals too are
   * opaque to Spark. A user can do anythng inside one of those, and
   * al  Spak can see is something like 4anon$1@604f1a67
   * 
   * However, In a database/Hive:
   * - We do declarative transformations on data.
   * - Specialized/structured. pre-defined operations
   * 
   * Fixed set of operations, fixed set of types they operate on.
   * Optimizations the norm.
   * 
   * ...
   * 
   * RDDS operate on unstructured data, and there are few limits on
   * computation; your computations are defined as functions that you've
   * written yourself, on your own data types.
   * 
   * But as we saw, we have to do all the optimization work ourselves!
   * 
   */

  val conf: SparkConf = new SparkConf().setAppName("Spark Pi").setMaster("local[*]")
  val sc: SparkContext = new SparkContext(conf)
  val rootLogger = Logger.getRootLogger()
  rootLogger.setLevel(Level.ERROR)
  def main(args: Array[String]) {

    print("\n\nGrouping and Reducing - groupBy:\n")

  }
}