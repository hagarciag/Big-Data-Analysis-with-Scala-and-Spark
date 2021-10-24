package week4

import org.apache.spark.sql.{ SparkSession, Row, Encoder, Encoders }
import org.apache.spark.sql.functions._
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.log4j.{ Level, Logger }

import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.TypedColumn

import week4.Abo
import week4.Loc

case class KeyValue(k: String, v: String)

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
   * Common Typed Transformations on Datasets
   * map, flatMap, filter, distinct, groupByKey, coalesce, repartition
   * 
   * Grouped Operations on Datasets
   * Like on DataFrames, Datasets have a special set of aggregation operations
   * meant to be used a call to groupByKey on a Dataset.
   * - calling groupByKey on a Datasets returns a KeyValueGroupedDataset
   * - KeyValueGroupedDataset contains a number of aggregation operations
   *                          which return Datasets.
   * 1. Call groupByKey on a Dataset, get back a KeyValueGroupedDataset.
   * 2. Use an aggregation operation on KeyValueGroupedDataset (return Datasets)
   * 
   * Note: using groupby on a Dataset, you will get back a RelationalGroupedDataset
   *       whose aggregation operators willl return a DataFrame. Therefore, be careful
   *       to avoid groupBy if you would like to stay in the Dataset API.
   * 
   * Some KeyValueGrouped Dataset Aggregation Operations.
   * reduceGroups, agg
   * 
   * Just like on DataFrames, there exists a general aggregation operation agg
   * defined on KeyValueGroupedDataset.
   * 
   * agg[U](col: TypedColumn[V, U]): Dataset[(K, U)]
   * 
   * The only thing a bit peculiar about this operation is its arguemt. What 
   * do we pass to it?
   * Typically, we simply select one of these operations from function, such as avg, choose a column
   * for avg to be computed on, and we pass it to agg.
   * 
   * Some KeyValueGroupedDataset (Aggregation) Operations
   * 
   * mapGroups: Applies the given function to each group of data. For each unique
   * group, the function will be passed the group key and an iterator
   * that contains all the elements of the group. The function can return an
   * element of arbitrary tyoe which will be returned as a new Dataset.
   * 
   * flatMapGroups: Applies the given function to each group of data. For each
   * unique group, the function will be passed the group. The function can return
   * an iterator containing elemtns of an arbitrary type, which will be return as a new Dataset
   * 
   * Note: at the time of writing, KeyValueGroupedDataset is marked as @Experimental
   * and @Evolving. Therefore, expect this API to fluctuate-it�s likely that new
   * aggregation operations will be added and others could be canged.
   * 
   * If you glance aroud the Dataset API docs, you might notice that Datasets
   * are missing an important transformation that we often used on RDD: reduceByKey.
   * 
   * Challenge:
   * Emulate the semantics of reduceByKey on a Dataset using Dataset operations
   * presented so far.
   * 
   * Find a way to use Datasets to achieve the same result that you would get
   * if you put this data into an ROD and called:
   * keyValuesRDD.reduceByey(_+_)
   * 
   * keyValuesDS1.groupByKey(p=>p._1)
   *         .mapGroups((k,vs)=>(k,vs.foldLeft("")((acc, p)=>acc + p._2)))
   *         .sort($"_1").show()
   * 
   * The only issue with this approach is this disclaimer in the API docs for
   * mapGroups:
   * This function does not support partial aggregtion, and as a result requieres
   * shuffling all the data in the Dataset. If an application intends to perform
   * an aggregation over each key, it is best to use the reduce function or an
   * org.apache.spark.sql.expressions#Aggregator.
   * 
   * Aggregators
   * A class that helps you generically aggregate data. Kind of like the aggregate
   * method we saw on RDDs.
   * 
   * class Aggregator[-IN, BUF, OUT]
   * 
   * - IN is the input type to the aggregator. When using an agregator after
   *      groupByKey, this is the type that represents the value in the key / value
   *      pair.
   * - BUF is the intermediate type during aggregation.
   * - OUT is the type of the output of the aggregation.
   * 
   * Encoders
   * They are what convert your data between JVM objects and Spark SQL's specilized
   * internal (tabular) representation. They're required by all Datasets!
   * 
   * Encoders are highly specialized, optimized code generators that generate
   * custom bytecode for serialization and deserialization of you data.
   * 
   * The serialized data is stored using Spark internal Tungsten binary format, allowing
   * for operations on serialzed data and improved memory utilization.
   * 
   * What sets them apart form regular Java of Kryo serialization:
   * - Limited to and optimal for promitives and case classes, Spark SQL data types, which are
   *   well-understood.
   * - They contain schema information, which makes these highly optimized code
   *   generators possible, and enables optimization based on the shape of the data.
   *   Since Spark understands the structure of data in Datasets, it can create
   *   a more optimal layout in memory when caching Datasets.
   * - Uses significantly less memory than Kryo/Java serialization
   * - >10x faster than Kryo serialization (Java seralization orders of magnitude slower)
   * 
   * En coders are what convert your data between JVM objects and Spark SQL's
   * specliazed internal representation. They're required by all Datasets!
   * 
   * Two ways to introduce encoders:
   * - Automatically (generally the case) via implicits from a SparkSession.
   * - Explicitly via org.apache.spark.sql.Encoder which contains a large selection
   * of methids for creating Enconders from Scala primitive types and Products.
   * 
   * Some examples of "Encoder" creation methods in "Encoders":
   * - INT/LONG/STRING etc, for nullable primitives.
   * - scalaInt/scalaLong/scalaByte etc, for Scala's primitives.
   * - product/tuple for Scala's Product and tuple types.
   * 
   * Common Dataset Actions
   * coolect, count, first, foreach, reduce, show y and take
   * 
   * When to use Datasets vs Data Frames vs RDDs?
   * Use Data sets when
   * - you have structured/ semi-structured data
   * - you want typesafety
   * - you need to work with functional APl s
   * - you need good performance, but it doesn't have to be the best
   * Use DataFrames when
   * - you have structured/semi-structured data
   * - you want the best possible performance, automatically optimized for you
   * Use RDDs when
   * - you have unstructured data
   * - you need to fine-tune and manage low-level details of R O D computations
   * - you have complex data types that cannot be serialized with E n coders
   * 
   * Catalyst Can't Optimize All Operations
   * 
   * Limitations of Datasets
   * 
   *Takeaways:
   *- When using Datasets with higher-order functions like map, you miss
   *  out on many Catalyst optimizations.
   *- When using Datasets with relational operations like select, you get
   *  all of Catalyst's optimizations.
   *- Though not all operations on Datasets benefit from Catalyst's
   *  optimizations, Tungsten is still always running under the hood of
   *  Datasets, storing and organizing data in a highly optimized way,
   *  which can result in large speedups over RDDs.
   * 
   * Limited Data Types
   * If your data can't be expressed by case classes/Products and standard
   * Spark SQL data types, it may be difficult to ensure that a Tungsten
   * encoder exists for your data type.
   *
   * E.g., you have an application which already uses some kind of complicated
   * regular Scala class.
   *
   * Requires Semi- Structured / Structured Data
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
    
    print("\n\nprintTreeString:\n")
    abosDF.head.schema.printTreeString()
    
    val abos = abosDF.collect()
    
    val abosAgain = abos.map{      row => (row(0).asInstanceOf[Int])    }
    
    //val abosAgain = abos.map((a: Row) => a.getAs[Seq[Row]](1).map{case Row(k: String, v: String) => (k, v)})
    
    print("\n\nabos:\n")
    print(abosAgain.foreach(println))
    
   
    val keyValuesDF = List((3,"Me"),(1,"Thi"),(2,"Se"),(3,"ssa"),(3,"-"),(2,"cre"),(2,"t"))
                        .toDF
    val keyValuesDS = keyValuesDF.map(row => row(0).asInstanceOf[Int] + 1)
    
    print("\r\n describe \r\n")
    
    print(keyValuesDS.describe())
    
    keyValuesDS.agg(avg($"value").as[Double]).collect().foreach(println)
    
    val keyValues = List((3,"Me"),(1,"Thi"),(2,"Se"),(3,"ssa"),(3,"-"),(2,"cre"),(2,"t"))
    val keyValuesDS1 = keyValues.toDS
    
    // Poor performance for shuffling
    keyValuesDS1.groupByKey(p=>p._1)
            .mapGroups((k,vs)=>(k,vs.foldLeft("")((acc, p)=>acc + p._2)))
            .sort($"_1").show()
           
    // Better option
    keyValuesDS1.groupByKey(p=>p._1)
            .mapValues(p => p._1)
            .reduceGroups((acc, str)=>acc + str).show
            
    // Aggregator option
    // Step 1: what should Aggregator's type parameters be?
    // Step 2: what should the rest of types be?
    // Step 3: implement the methos!
    // Step 4: tell Spark which Encoders you need.
    // Step 5: pass it to your aggregator!
    val strConcat = new Aggregator[(Int, String), String, String]{
    def zero: String = ""
    def reduce(b: String, a: (Int, String)): String = b + a._2
    def merge(bl: String, b2: String): String = bl + b2
    def finish(r: String): String = r
    override def bufferEncoder: Encoder[String]=Encoders.STRING
    override def outputEncoder: Encoder[String]=Encoders.STRING
    }.toColumn
  
    keyValuesDS1.groupByKey(pair => pair._1)
      .agg(strConcat.as[String]).show            
    
            
  }
}