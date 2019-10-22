package week4

import org.apache.spark.sql.{ SparkSession, Row }
import org.apache.spark.sql.types.{ StructField, StructType, StringType }
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
//import org.apache.spark.SparkContext._
import org.apache.log4j.{ Level, Logger }
//import org.apache.spark.rdd.RDD
import week4.Abo
import week4.Loc

object Datasets {
  /*
   * Datasets
   * DataFrames are actually Datasets
   * 
   * type DataFrame = Dataset[Row]
   * 
   * What the heck is a Dataset?
   * 
   * - Datasets can be thought of as typed distributed collections of data.
   * - Dataset API unifies the DataFrame RDD APIs. Mix and match!
   * - Datasets require structured/semi-structured data. Schemas and Encoders core part
   *   of Datasets.
   *   
   * Think of Datasets as a compromise between RDD & DataFrames.
   * You get more type information on Datasets than on DataFrames, and you get more
   * optimizations on Datasets than you get on RDDs.
   * 
   * Datasets are a something in the middle between Dataframes and RDDs.
   * - You can still use relational DataFrame operations as we leaned in
   * previous sessions on Datasets.
   * - Datasets add more typed operations that can be used as well.
   * - Datasets let you use higher-order functions like map, flatMap, filter again!
   * 
   * Datasets can be used when you want a mix of functional and relational
   * transformations while benefiting from some of the optimizations on DataFrames.
   * And we've almost got a type safe APIas well.
   * 
   * Creating Datasets.
   * - From a DataFrame
   * Just use the toDS convenience method.
   * 	myDF.toDS
   * Note that often it's desirable to read in data from JSON from a file, which can be done
   * with the read method on the SparkSession object like we saw in previous sessions, and
   * then converted to Dataset:
   * 	val myDS = spark.read.json("people.json").as[Person]
   * 
   * - From an RDD
   * Just use the toDS convenience method.
   * myRDD.toDS
   * 
   * - From common Scala types.
   * Just use toDS convenience method.
   * List("yay", "ohnoes", "hooray!").toDS
   * 
   * Typed Columns
   * Recall the column type from DataFrames. On Datasets, typed operations tend
   * to act on TypedColumn instead.
   * 
   * To create a TypedColumn, all you have to do is call as[...] on your (untyped)
   * Column:
   * $"price".as[Double] // this now represents a TypedColumn.
   * 
   * Transformations on Datasets.
   * The Dataset API includes both untyped and typed transformations...
   * - untyped transformations: the transformations we learned on DataFrames.
   * - typed transformations: typed variants of many DataFrame transformations + 
   * additional transfomations such as RDD-like higher-order functions map,
   * flatMap, etc.
   * 
   * These AIs are integrated. You can call a map on a DataFrame and get back a Dataset,
   * for example.
   * Caveat: not every operation you know from RDDs are available on Datasets, and not all
   * operations look 100% the same on Dataserts as they did on RDDs.
   * 
   * But remember, you may have to explicitly provide type information when going
   * from a DataFrame to a Dataset via typed transformations.
   * 
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
    
    val abos = abosDF.collect()
    
    val abosAgain = abos.map{
      row => (row(0).asInstanceOf[Int])
    }
    
    print("\n\nabos:\n")
    print(abosAgain.foreach(println))
    
    
    val keyValuesDF = List((3,"Me"),(1,"Thi"),(2,"Se"),(3,"ssa"),(3,"-"),(2,"cre"),(2,"t")).toDF
    val res = keyValuesDF.map(row => row(0).asInstanceOf[Int] + 1)

   
  }
}