package week4

import org.apache.spark.sql.{ SparkSession, Row }
import org.apache.spark.sql.types.{ StructField, StructType, StringType }
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.log4j.{ Level, Logger }
import org.apache.spark.rdd.RDD
import week3.CFFPurchase

object SparkSQL {
  /*
   * Spark SQL
   *
   * It has threee main goals:
   * 1. Support relational processing both within Spark programs (on RDDS)
   *    and on external data soruces with a friendly API.
   * 2. High performance, achieved by using techniques from research in databases.
   * 3. Easily support new data sources such as semi-stuctured data and external
   *    databases.
   *
   * Spark SQL is a component of the Spark stack.
   * - It is a Spark module for structured data processing.
   * - It is implemented as a library on top of Spark.
   *
   * Three main APIs:
   * - SQL literal suntax
   * - DataFrames
   * - Datasets
   *
   * Two specialized backend components:
   * - Catalyst: query optimizer.
   * - Tungsten: off-heap serializer.
   *
   * DataFrame is Spark SQL's core abstraction.
   *
   * Conceptually equivalent to a table in a relational database.
   *
   * DataFrames are, conceptually, RDDs full of records with a known schema.
   *
   * DataFrames are untyped!
   * That is, the Scala compiler doesn't check the types in its schema!
   *
   * DataFrames contain Rows which can contain any schema.
   *
   * Transformations on DataFrames are also known as untyped transformations.
   *
   * SparkSession is the SparkContext for Spark SQL.
   *
   * DataFrames can be created in two ways:
   * 1. Form an existing RDD: Either with schema inference, or with an explicit schema.
   *    a. This can be done by .toDF(<>). If no columns are provided, Spark
   *    will fill the column names with numbers.
   *    If you already have an RDD containing some kind of case class instance
   *    then Spark can infer the attributes from the case class's fields.
   *    b. It takes 3 steps:
   *     - Create an RDD of Rows from the original RDD.
   *     - Create the schema represented by StructType matching the structure
   *       of Rows in the RDD created in Step 1.
   *     - Apply the schema to the RDD of Rows via createDataFrame method
   *       provided by SpakSession.
   * 2. Reading in a specific data source from file: Common structured or
   *    semi-structured formats such as JSON.
   *    There are many files supported:
   *    - JSON
   *    - CSV
   *    - Parquet
   *    - JDBC
   *
   * SQL literals
   * Once you have a DataFrame to operate on, you can now freely write
   * familiar SQL syntax to operate on your dataset!
   *
   * The SQL statements available to you are largely what's avaialble in
   * HiveQL. This includes standard SQL staments such as:
   * SELECT, FROM, WHERE, COUNT, HAVING, GROUP BY, ORDER BY, SORT BY,
   * DISTINCT, JOIN, LEFT|RIGHT|FULL OUTER JOIN, and subqueries.
   */

  val conf: SparkConf = new SparkConf().setAppName("Spark Pi").setMaster("local[*]")
  val sc: SparkContext = new SparkContext(conf)

  val spark = SparkSession.builder().appName("My App").getOrCreate()
  import spark.implicits._

  val rootLogger = Logger.getRootLogger()
  rootLogger.setLevel(Level.ERROR)

  def main(args: Array[String]) {
    /////////INFER SCHEMA FROM RDD///////////////////////////////////
    val tupleRDD = sc.parallelize(List("hello", "world", "good", "morning"))

    //Create DataFrame from existing RDD without explicit schema by toDF.
    val tupleDF = tupleRDD.toDF("word")

    print("\n\nRDD to DataFrame inferring schema:\n")
    print(tupleDF.show(5))

    /////////INFER SCHEMA FROM RDD 2///////////////////////////////////
    val tupleRDD2 = sc.parallelize(List(("hello",1), ("world",2), ("good",3), ("morning",4)))

    //Create DataFrame from existing RDD without explicit schema by toDF.
    val tupleDF2 = tupleRDD2.toDF("word", "Value")

    print("\n\nRDD to DataFrame infering schema:\n")
    print(tupleDF2.show(5))

    //////////SCHEMA INFERRED FROM CASE CLASS///////////////////////////////
    val purchases = List(
      CFFPurchase(100, "Geneva", 22.25),
      CFFPurchase(300, "Zurich", 42.10),
      CFFPurchase(100, "Fribourg", 12.40),
      CFFPurchase(200, "St. Gallen", 8.20),
      CFFPurchase(100, "Lucerne", 31.60),
      CFFPurchase(300, "Basel", 16.20))

    val purchasesRDD = sc.parallelize(purchases)

    //Create a DF from an RDD containing a case class.
    val purchasesDF = purchasesRDD.toDF

    print("\n\nRDD to DataFrame taking schema from case class:\n")
    print(purchasesDF.show(5))

    ///////////EXPLICIT SCHEMA////////////////////////////////////
    // This is how a file is read into Spark
    val purchases1 = List(
      "100,Geneva,22.25", "300,Zurich,42.10", "100,Fribourg,12.40", "200,St. Gallen,8.20", "100,Lucerne,31.60", "300,Basel,16.20")

    val purchasesRDD1 = sc.parallelize(purchases1)

    // The schema is encoded in a string
    val schemaString = "id city price"

    // Generate the schema based on the string of schema
    val fields = schemaString.split(" ").map(fieldName => StructField(fieldName, StringType, nullable = true))
    val schema = StructType(fields)

    // Convert records of the RDD (purchases) to Rows
    val rowRDD = purchasesRDD1.map(_.split(",")).map(attributes => Row(attributes(0), attributes(1), attributes(2).trim))

    // Apply the schema to the RDD
    val purchaseDF = spark.createDataFrame(rowRDD, schema)

    print("\n\nRDD to DataFrame proving schaema:\n")
    print(purchaseDF.show(5))

    ///////////INFER SCHEMA FROM THE FILE/////////////////////////////
    // Using the sparkSession object, you can read files.
    val df = spark.read.json("./resources/people.json")

    print("\n\nCreating DataFrame from JSON file:\n")
    print(df.show(5))
    print(df.show())
    print(df.collect.foreach(println))

    //////////SQL LITERALS///////////////////////////////////////////
    purchasesDF.createOrReplaceTempView("purchases")

    var query = spark.sql("""
                              SELECT customerid, SUM(price) AS total
                              FROM purchases
                              GROUP BY customerid
                              ORDER BY total DESC
                          """)

    print(query.show(5))

  }
}