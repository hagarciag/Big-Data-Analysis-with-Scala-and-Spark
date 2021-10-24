package week1

object Lecture_1_RDDs_Sparks_Distributed_Collection {
  /*
  RDDs seem a lot like immutable sequential or parallel Scala collections

// Simplification of a RDD class...
abstract class RDD[TJ {
def map[U](f: T => U): RDD[UJ = ...
def flatMap[UJ(f: T => TraversableOnce[UJ): RDD[UJ = ...
def filter(f: T => Boolean): RDD[TJ = ...
def reduce(f: (T, T) => T): T = ... 
}

Most operations on RDDs, like Scala's immutable List, and Scala's parallel collections, are higher-order functions.
That is, methods that work on RDDs, taking a function as an argument, and which typically return RDDs.

Combinators on Scala
parallel/ sequential collections:        Combinators on RDDs:
map                                      map
flatMap                                  flatMap
filter                                   filter
reduce                                   reduce
fold                                     fold
aggregate                                aggregate



While their signatures differ a bit, their semantics (macroscopically) are
the same:
map[BJ(f: A=> B): List[BJ // Scala List
map[B](f: A=> B): RDD[BJ // Spark ROD
flatMap[BJ(f: A=> TraversableOnce[B]): List[BJ // Scala List
flatMap[BJ(f: A=> TraversableOnce[B]): RDD[BJ // Spark ROD
filter(pred: A=> Boolean): List[AJ // Scala List
filter(pred: A=> Boolean): RDD[AJ // Spark ROD 

// No by name
aggregate[BJ(z: => B)(seqop: (B, A)=> B, combop: (B, B) => B): B // Scala
// By name because it is for distributed parallel
aggregate[BJ(z: B)(seqop: (B, A)=> B, combop: (B, B) => B): B // Spark RDD 

Using RDDs in Spark feels a lot like normal Scala sequential/parallel
collections, with the added knowledge that your data is distributed across
several machines.
Example:
Given, val encyclopedia: RDD[String], say we want to search all of
encyclopedia for mentions of EPFL, and count the number of pages that
mention EPFL. 

val result= encyclopedia.filter(page => page.contains("EPFL"))
.count()


Example: Word Count...

The "Hello, World!" of programming with large-scale data.

val rdd = spark.textFile("hdfs:// ... ")

val count = rdd.flatMap(line => line.split(" "))
				.map(word => (word, 1))
				.reduceByKey(_ + _)


RDDs can be created in two ways:
	Transforming an existing RDD .
	From a SparkContext ( or SparkSession) object. 

Transforming an existing RDD.
Just like a call to map on a List returns a new List, many higher-order
functions defined on RDD return a new RDD. 

From a SparkContext (or SparkSession) object .
The SparkContext object (renamed SparkSession) can be thought of as
your handle to the Spark cluster. It represents the connection between the
Spark cluster and your running application. It defines a handful of
methods which can be used to create and populate a new RDD:
	parallelize: convert a local Scala collection to an RDD .
	textFile: read a text file from HDFS or a local file system and return
an RDD of String
*/
}