package week4

import org.apache.spark.sql.{ SparkSession, Row }
import org.apache.spark.sql.types.{ StructField, StructType, StringType }
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
//import org.apache.spark.SparkContext._
import org.apache.log4j.{ Level, Logger }
//import org.apache.spark.rdd.RDD

case class Abo(id: Int, v: (String, String))
case class Loc(id: Int, v: String)

object Dataframes_2 {
  /*
   * Dataframes
   *
   * So far, we got an intuiton of what DataFrames are, and we learned how
   * to create them, and how to do many important transformations and
   * aggregations on them.
   *
	 * Cleaning Data with DataFrames
	 * Sometimes you may have a data set with null or NaN values. In these cases
	 * it's often desirable to do one of the following:
	 * - drop rows/records with unwanted values like null or Nan
	 * - replace certain values with a constant
	 *
	 * Droping records with unwanted values:
	 * - drop() drops rows that contain null or Nan values in any column and
	 *   returns a new DataFrame.
	 * - drop("all") drops rows that contain null or Nan values in all column and
	 *   returns a new DataFrame.
	 * - drop(Array("10", "name")) drops rows that contain null or Nan values
	 *   in the specified columns and returns a new DataFrame.
	 *
	 * Replacing unwanted values:
	 * - fill(0) replaces all occurrences of null or Nan in numeric columns
	 *   with specified value and returns a new DataFrame.
	 * - fill(map("minBalance" -> 0) replaces all occurrences of null or NaN
	 *   in specified column with specified value and returns a new DataFrame.
	 * - replace(Array("10"), map(1234 -> 8923)) replaces specified value
	 *   (1234) in specified column (id) with specified replacement value
	 *   (8923) and returns a new DataFrame.
	 *
	 * Like RDDs, DataFrames also have their own set of actions.
	 * We've even used one several times already.
	 *
	 * collect(): Array[ROW] Returns an array that contains all of Rows in this
	 *            DataFrame.
	 * count(): Long Returns the number of rows in the DataFrame.
	 * first(): Row$#%: Row Returns the first row in the DataFrame
	 * show():"$"#$ Disply the top 20 ros of DataFrame in a tabular form.
	 * take(n): Retuns the first n rows in the DataFrame.
	 *
	 * Joins on DataFrames are similar to those on Pair RDDs, with tne one
	 * major usage difference that, since DataFrames aren't key/value pairs,
	 * we have to specify which columns we should join on.
	 *
	 * Several types of joins are available:
	 * inner, outer, left_outer, right_outer, leftsemi
	 *
	 * Optimizations
	 * - Reordering operations
	 * Laziness + structures gives us the ability to analyze and rearrange
	 * DAG of computation/the logical operations the user would like to do,
	 * before they'e executed.
	 * 
	 * E.g., Catalyst can decide to rearrange and fuse together filter operations, pushing all
	 * filters early as possible, so expensive operations later are done on less
	 * data.
	 * 
	 * - Reduce the amount of data we must read.
	 * Skip reading in, serializing, and sending around parts of the data set that aren't needed
	 * for computation.
	 * E.g. Imagine Scala object containing many fields unnecesary to our
	 * computation. Catalyst can narrow down and select, serialize, and send around
	 * only relevant columns of our data set.
	 * 
	 * - Pruning unneeded partitioning.
	 * Analyze DataFrame and filter operations to figure out and skip partitions
	 * that are unneeded in our computation.
	 * 
	 * Tungsten
	 * Since our data types are restricted to Spark SQL data types, Tungsten can provide:
	 * - highly-speacialized data encoders
	 * Tugnsten can take schema information and tightly pack serialized data into
	 * memori. Tis means more data can fit in memory, and faster serialization/
	 * deserialization (CPU bound task)
	 * 
	 * - column-based
	 * Based on the observation that most operations done on table tend to be
	 * focused on specific columns/attributes of th data set. Thus, when storing
	 * data, group data by column instead of row for faster lookups of data associated
	 * with specific attributes/columns.
	 * 
	 * Well-known to be more efficient across DBMS
	 * 
	 * - off-heap (free from garbage collection overhead)
	 * Regions of memory off the heap, manually managed by Tungsten, so as to avoid
	 * garbage collection overhead and pauses.
	 * 
	 * Taken together, Catalyst and Tungsten offer ways to significantly speed up your code,
	 * even if you write it inefficiently initially.
	 * 
	 * Limitations of DataFrames...
	 * - Untyped!
	 * Your code compiles, but you get runtime exceptions when you attempt to
	 * run a query on a column that doesn't exist.
	 * 
	 * Would be nice if this was caught at compile time like we're used to in Scala!
	 * 
	 * - Limited Data Types
	 * If you data can't be expressed by case classes/Products and standard
	 * Spark SQL data types, it may be difficult to ensure that a Tungsten encoder
	 * exists for yout data type.
	 * 
	 * E.g. you have an application which already uses some kind of complicated regultar Scala class
	 * 
	 * - Requires semi-structured/Structured Data
	 * If your unstructured data cannot be reformulated to adhere to some kind
	 * of schema, it would be better to use RDDs.
	 * 
   */

  val conf: SparkConf = new SparkConf().setAppName("Spark Pi").setMaster("local[*]")
  val sc: SparkContext = new SparkContext(conf)

  val spark = SparkSession.builder().appName("My App").getOrCreate()
  import spark.implicits._

  val rootLogger = Logger.getRootLogger()
  rootLogger.setLevel(Level.ERROR)

  def main(args: Array[String]) {

    val as = List(Abo(101, ("Ruetli", "AG")), Abo(102, ("Brelaz", "DemiTarif")),
      Abo(103, ("Gress", "Demi Tari fVi sa")), Abo(104, ("Sc hat ten", "Demi Tari f")))

    val abosDF = sc.parallelize(as).toDF
    
    print("\n\nabosDF:\n")
    abosDF.show()
    
    print("\n\nabosDF.collect.foreach(println):\n")  
    abosDF.collect.foreach(println)

    print("\n\nabosDF.take(2).foreach(println):\n")  
    abosDF.take(2).foreach(println)

    val ls = List(Loc(101, "Bern"), Loc(101, "Thun"), Loc(102, "Lausanne"), Loc(102, "Geneve"),
      Loc(102, "Nyon"), Loc(103, "Zurich"), Loc(103, "St-Gallen"), Loc(103, "Chur"))

    val locationsDF = sc.parallelize(ls).toDF

    print("\n\nlocationsDF:\n")
    locationsDF.show()

    print("\n\njoin:\n")
    val trackedCustomersDF = abosDF.
              join(locationsDF, abosDF("id")===locationsDF("id"))
              
    trackedCustomersDF.show()
     
    print("\n\nleft_outer:\n")
    val abosWithOptionalLocationsDF = abosDF.
              join(locationsDF, abosDF("id")===locationsDF("id"), "left_outer")
              
    abosWithOptionalLocationsDF.show()
    
  }
}