package week4

import org.apache.spark.sql.{ SparkSession, Row }
import org.apache.spark.sql.functions._
//import org.apache.spark.sql._
import org.apache.spark.sql.types.{ StructField, StructType, StringType, IntegerType }
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.log4j.{ Level, Logger }
import org.apache.spark.rdd.RDD
import week3.CFFPurchase

object Dataframes_1 {
  /*
   * Dataframes
   *
   * So far, we got an intuiton of what DataFrames are, and we learned how
   * to create them. We also saw that if we have a DataFrame, we use SQL
   * syntax and do SQL queries on them.
   * 
   * DataFrames have their own APIs as well!
   * 
   * DataFrames in a Nutshell. DataFrames are...
   * - A relational API over Spark's RDDs
   * Because sometimes it is more convenient to use declarative relational
   * APIs than functional APIs for analysis jobs.
   * 
   * - Able to be automatically aggressively optimized
   * SparkSQL applies years of research on relational optimizations in the 
   * databases community to Spark.
   * 
   * - Untyped
   * The elements within DataFrames are Rows, which are not parametrized
   * by a type. Therefore, the Scala compiler cannot type check Spaek SQL
   * schemas in DataFrames.
   * 
   * To enable optimization opportunities, Spark SQL's DataFrames operate
   * on a restricted set of data types.
   * 
   * Basic Spark SQL Data Types:
   * Scala Type							 SQL Type					Details
   * Byte                    ByteType         1 byte signed integers (-128,127)
   * Short                   Short Type       2 byte signed integers (-32768,32767)
   * Int                     IntegerType      4 byte signed integers (-2147483648,2147483647)
   * Long                    LongType         8 byte signed integers
   * java.math.BigDecimal    Decimal Type     Arbitrary precision signed decimals
   * Float                   Float Type       4 byte floating point number
   * Double                  Double Type      8 byte floating point number
   * Array[Byte]             BinaryType       Byte sequence values
   * Boolean                 Boolean Type     true/false
   * Boolean                 Boolean Type     true/false
   * java.sql.Timestamp      Times tamp Type  Date containing year, month, day, hour, minute, and second .
   * java.sql.Date           DateType         Date containing year, month, day.
   * String                  StringType       Character string values (stored as UTF8)
   * 
   * To enable optimization opportunities, spark SQL's DataFrames operate
   * on a restricted set of data types.
   * 
   * Complex Spark SQL Data Types:
   * Scala Type  	SQL Type
	 * Array[TJ    	ArrayType(elementType, containsNull)
	 * Map[K, VJ   	MapType(keyType, valueType, valueContainsNull)
	 * case class	 	StructType(List[StructFields])
	 * 
	 * Arrays
	 * Array of only one type of elements (elementType). containsNull is set
	 * to true if the elements in ArrayType value can have nulls.
	 *
	 * // Scala type		// SQL type
	 * Array[String]		ArrayType(StringType, true)	 * Maps
	 * 
	 * 
	 * Map of key/value pairs with two types of elements. valuecontainsNull is
	 * set to true if the elements in MapType value can have null values.
	 * 
	 * // Scala type			// SQL type
	 * Map[Int,String]		MapType(IntegerType, StringType, true)
	 * 
	 * 
	 * Structs
	 * Struct type with list of possible fields of different types. containsNull
	 * is set to true if the elements in StructFieds can have null values.
	 *  // Scala type									 								// SQL type
	 *  case class Person(name: String, age: Int)	    StructType(List(StructField("name", StringType, true),
	 * 												 												StructField("age", IntegerType, true)))
	 * 
	 * Important.
	 * In order to access any of these data types, either basic or complex,
	 * you must first import Spark SQL types!
	 * 
	 * import org.apache.spark.sql.types._
	 * 
	 * When introduced, the DataFrames API introduced a number of relational
	 * operations.
	 * The main difference between the RDD API and the DataFrame API was that
	 * DataFrame APIs accept Spark SQL expressions, intead of arbitrary user-defined
	 * function literals like we were used to on RDDS. This allows the optimizer
	 * to understand what the computation represents, and for example with filter,
	 * it can often be used to skip reading unnecesary records.
	 * 
	 * DataFrames API: Similar-looking to SQL. Example methods include:
	 * - select
	 * - where
	 * - limit
	 * - orderBy
	 * - groupBy
	 * - join
	 * 
	 * Getting a look at your data
	 * Before we get into transformations and actions on Dataframes, let's first
	 * look at the ways we can have a look at our data set.
	 * 
	 * show()  pretty-prints DataFrame in tabular form. Shows first 20 elements
	 * printSchema prints the schema of your DataFrame in a tree format
	 * 
	 * like on RDDs, transformations on DataFrames are (1) operations which return
	 * a DataFrame as a result, and (2) are lazily evaluated.
	 * 
	 * Some common transformations include:
	 * 
	 * def select(col: String, cols: String*): DataFrame
	 * // selects a set of named columns and returns a new DataFrame with these
	 * //columns as a result.
	 * 
	 * def agg(expr: Column, exprs: Column*): DataFrame
	 * // performs aggregations on a series of columns and returns a new DataFrame
	 * // with the calculated output.
	 * 
	 * def groupBy(col1: String, cols: String*): DtaFrame // simplified
	 * // groups the DataFrame using the specified columns. Intended to be used before an aggregation.
	 * 
	 * def join(right: DataFrame): DataFrame // simplified
	 * //inner join with another DataFrame
	 * 
	 * // Other transformations include: filter, limit, orderBy, where, as, sort, union, drop,
	 * // amongst others.
	 * 
	 * Specifying Columns
	 * 
	 * As you might have observed from the previous slide, most methods take a
	 * parameter of type Column or String, always referring to some attribute/column in
	 * the data set.
	 * 
	 * Most methods on DataFrames tend to some well-understood, pre-defined
	 * operation on a column of the data set
	 * 
	 * You can select and work with columns in three ways:
	 * 1. Using $-notation
	 * 2. Referring to the Dataframe
	 * 3. Using SQL query string
	 * 
	 * Filtering in Spark SQL
	 * The DataFrame API makes two methods available for filtering:
	 * filter and where (from SQL). They are equivalent!
	 * 
	 * Grouping and Aggregating on Data Frames
	 * 
	 * One of the most common tasks on tables is to (1) group data by a certain
	 * attribute, and then (2) do some kind of aggregation on it like a count.
	 * 
	 * For grouping & aggregating, Spark SQL provides:
	 * - a groupBy function which returns a RelationalGroupedDataset
	 * - which has several standard aggregation functions defined on it like count,
	 *   sum, max, min, and avg.
	 *   
	 * How to group an d aggregate?
	 * - Just call groupBy on specific attribute/column(s) of a DataFrame,
	 * - followed by a call to a method on RelationalGrouped Dataset like count, max,
	 *   or agg (for agg, also specify which attribute/column(s) subsequent
	 *   spark.sql.functions like count, sum, max, etc, should be called upon.)
	 *   
	 * After calling groupBy, methods on RelationalGroupedDataset:
	 * To see a list of all operations you
	 * can call following a groupBy, see the API docs for RelationalGroupedDataset.
	 * https://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.sql.RelationalGroupedDataset
	 * 
	 * Methods within agg:
	 * Examples include: min, max, sum, mean, stddev, count, avg, first, last. To
	 * see a list of all operations you can call within an agg, see the API docs for
	 * org.apache.spark.sql.functions.
	 * https://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.sql.functions$
	 * 
   */

  val conf: SparkConf = new SparkConf().setAppName("Spark Pi").setMaster("local[*]")
  val sc: SparkContext = new SparkContext(conf)

  val spark = SparkSession.builder().appName("My App").getOrCreate()
  import spark.implicits._

  val rootLogger = Logger.getRootLogger()
  rootLogger.setLevel(Level.ERROR)

  def main(args: Array[String]) {

    // This is how a file is read into Spark
    val purchases1 = List(
      "100,Geneva,22.25", "300,Zurich,42.10", "100,Geneva,12.40", "200,St. Gallen,8.20", "100,Lucerne,31.60", "300,Basel,16.20")

    // Generate an RDD from a Scala list
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
  
    // pretty-prints DataFrame in tabular form. Shows first 20 elements
    print("\n\nshow:\n")
    purchaseDF.show()

    // prints the schema of your DataFrame in a tree format
    print("\n\nprintSchema:\n")
    purchaseDF.printSchema()
    
    print("\n\nfilter using $-notation:\n")
    purchaseDF.filter($"price" >= 31).show()
    
    print("\n\nfilter using '-notation:\n")
    purchaseDF.filter('price >= 31).show()
    
    print("\n\nfilter referring to the Dataframe:\n")
    purchaseDF.filter(purchaseDF("price") >= 31).show()
    
    print("\n\nfilter using SQL query string:\n")
    purchaseDF.filter("price >= 31").show()
    
    print("\n\nfilter and where are equivalent using SQL query string:\n")
    purchaseDF.filter("price >= 31").show()
    purchaseDF.where("price >= 31").show()
    
    print("\n\nfilter can be more complex too:\n")
    purchaseDF.filter(($"price" >= 31) && ($"city" === "Zurich")).show()
    
    print("\n\ngroup by 'id' and sum by 'price' with agg:\n")
    // It's porssible to use $"" ir '.
    purchaseDF.groupBy('id).agg(sum($"price")).show()
    
    print("\n\ngroup by 'id' and count without agg:\n")
    purchaseDF.groupBy($"id").count().show()
    
    val purchaseDF2 = purchaseDF.selectExpr("id", "cast(price as int) price")

    print("\n\nThe most expensive ticket by 'id':\n")
    purchaseDF2.groupBy($"id").max("price").show()
        
    print("\n\nThe least expensive ticket by 'id':\n")
    purchaseDF2.groupBy($"id").min("price").show()
    
    print("\n\nNumber of records per 'id' and 'city':\n")
    purchaseDF.groupBy($"id", 'city).count().orderBy($"city", $"count".desc).show()
    purchaseDF.groupBy($"id", 'city).agg(count('id)).orderBy($"city", $"count(id)".desc).show()
    
    print("\n\nNumber of records per 'id' and 'city'. The count operation is renamed and sort is done by count and city:\n")
    purchaseDF.groupBy($"id", 'city).agg(count('id) as "count").orderBy($"count".desc, $"city".desc).show()
    
    //////////SQL LITERALS///////////////////////////////////////////
    purchaseDF.createOrReplaceTempView("purchases")

    var query = spark.sql("""
                              SELECT id, SUM(price) AS total
                              FROM purchases
                              GROUP BY id
                              ORDER BY total DESC
                          """)

    query.show(5)

  }
}