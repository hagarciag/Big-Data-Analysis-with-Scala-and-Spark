package week1

object RDDs_Sparks_Distributed_Collection {
  /*
  How to create an RDD?
  RDDs can be created in two ways:
  - Transforming an existing RDD.
  - From a SparkContext (or SparkSession) object.
  
  From a SparkContext (or SparkSession) object
  The SparkContext object (renamed SparkSession) can be thought of as
  your handle to the Spark cluster. It represents the connection between the
  Spark cluster and your running application. It defines a handful of
  methods which can be used to create and poplate a new RD:
  - parallelize: convert a local Scala collection to an RDD.
  - textFile: read a text file from HDFS or a local file system and return
    an RDD of String.
    
  Note: before Spark 2.0 SparkContext held all Spark configuration options,
  after 2.0 SparkContext has the same options, but SparkSession start being
  used which the same options of SparkContext, and also has others suc as
  datasets, dataframe, SQL, Hive, and streaming. 
  */
  //val data = spark.read.option("header", true).option("delimiter", ",") .option("inferSchema", true).csv("Tweet.csv")
}