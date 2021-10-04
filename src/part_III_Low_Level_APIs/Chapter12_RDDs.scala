package part_III_Low_Level_APIs

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.log4j.{ Level, Logger }

import org.apache.spark.rdd.RDD

object Chapter12_RDDs {

  /*
   * When to use the low-level APIs (RDD)?
   * - You need some funcionality that you cannot find in the higher-level (structured) APIs; for example, if you neeed very tight control over physical data placement
   *   across the cluster (custom partitioner).
   *   
   * - You need to maintain some legacy codebase written using RDDs
   * 
   * - You need to do some custom shared variables manipulation
   * 
   * 						Structured APIs
   * Datasets			Dataframes			SQL
   * 
   * 
   * 						Low-level APIs
   * RDDs			Distributed Variables
   * */
  val spark = SparkSession.builder.master("local").appName("My App").getOrCreate()
  import spark.implicits._
  
  val sc: SparkContext = spark.sparkContext
  
  val rootLogger = Logger.getRootLogger()
  rootLogger.setLevel(Level.ERROR)
  
  def main(args: Array[String]) {
  	//Dataset to RDD
    spark.range(500).rdd 
    
    //Dataset to DF to RDD
    spark.range(10).toDF().rdd.map(rowObject => rowObject.getLong(0))
    
    //RDD to DF
    spark.range(10).rdd.toDF()
    
    val myCollection = "Spark The Definitive Guide : Big Data Processing Made Simple".split(" ")
    val words = sc.parallelize(myCollection, 2)
    words.setName("myWods") 
    println(words.name)
    
    //TRANSFORMATIONS
    
    //distinct
    println(words.distinct.count())
    
    //filter
    def startsWithS(individual: String) = {
      individual.startsWith("S")
    }
    
    val wordsS = words.filter(word => startsWithS(word)).collect()
    println(wordsS.toList.toString()) 

    //map - returns an RDD of triplets
    val words2 = words.map(word => (word, word(0), word.startsWith("S")))
    
    // where the third argument is true
    println(words2.filter(word => word._3).take(5).toList.toString())
    
    //flatMap
    println(words.flatMap(word => word.toSeq).take(5).toList.toString())
    
    //sort
    println(words.sortBy(word => word.length() * -1).take(5).toList.toString())
    
    //randomSplits
    println(words.randomSplit(Array[Double](0.5, 0.5))(0).take(5).toList.toString())
    
    
    //TRANSFORMATIONS
    
    //reduce
    println(sc.parallelize(1 to 20).reduce(_ + _))
    
    def wordLenghtReduce(leftWord:String, rightWord:String): String = {
      if(leftWord.length() > rightWord.length())
        return leftWord
      else
        return rightWord
    }
    
    println(words.reduce(wordLenghtReduce))
    
    //count
    println(words.count())
    
    //count
    println("count...")
    val confidence = 0.99
    val timeoutMilliseconds = 8
    println(words.countApprox(timeoutMilliseconds, confidence).initialValue.mean)
    println(words.countApprox(timeoutMilliseconds, confidence).getFinalValue().mean)
    
    //count
    println(words.countApproxDistinct(0.05))
    
    //countByValue
    println(words.countByValue)
    
    //countByValueApprox
    println(words.countByValueApprox(1000, 0.95))
    
    //first
    println(words.first())
    
    //max and min
    println("max and min...")
    println(sc.parallelize(1 to 20).max())
    println(sc.parallelize(1 to 20).min())
    
    //take - This works by first scanning one partition and then using the results from that partition to estimate the number of
    //additional partitions needed to satisfy the limit.
    println(words.take(5).toList.toString())
    
    // takeOrdered selects the top values according to the implicit ordering. Returns the first k (smallest) elements from this RDD as defined by the specified implicit Ordering[T] and maintains the ordering. This does the opposite of top.
    println(words.takeOrdered(5).toList.toString())
    // top is effectively the opposite of takeOrdered. Returns the top k (largest) elements from this RDD as defined by the specified implicit Ordering[T] and maintains the ordering. This does the opposite of takeOrdered.
    println(words.top(5).toList.toString())
    
    // You can use takeSample to specify a fixed-size random sample from your RDD. You can specify
    //whether this should be done by using withReplacement, the number of values, as well as the random seed
    val withReplacement = true
    val numberToTake = 6
    val randomSeed = 100L
    println(words.takeSample(withReplacement, numberToTake, randomSeed).toList.toString())
    
    //256
    
  }
  
}