package week1

object Lecture_2_Transformations_And_Actions {
  /*
  Recall transformers and accessors from Scala sequential and parallel
  collections?
  
  Transformers. Return a new collection as results. (Not single values.)
  Example: map, filter, flatMap, groupBy.
  
  Accesors: Return single values as resultas. (Not collections.)
  Examples: reduce, fold, aggregate.
  
  Similarly, Spark defines transformations and actions on RDDs.
  They seem similar to transformers and accesors, but there are some
  important differences.
  
  Transformations. Return a new RDDs as results.
  They are lazy, their result RDD is not immediately computed.
  
  Actions. Compute a result based on an RDD, and either returned or
  saved to an external storage system (e.g. HDFS).
  They are eager, their result is immediately computed.
  
  Common transformations(lazy operations) in the wild...
  map
  flatMap
  filter
  distinct

  Common actions(eager operations) in the wild...
  collect
  count
  take
  reduce
  foreach
  
  Example:
  
	Let's assume that we have an RDD[String] which contains gigabytes of
	logs collected over the previous year. Each element of this ROD represents
	one line of logging.
	Assuming that dates come in the form, YYYY-MM-DD:HH:MM:SS, and errors
	are logged with a prefix that includes the word 11
	error" ...
	How would you determine the number of errors that were logged in
	December 2016?

	val lastYearsLogs: RDD[String] = ...
	val numDecErrorLogs = lastYearsLogs.filter(lg => lg.contains("2016-12") && 
	                      lg.contains("error")).count()
	
	Example:
	val lastYearslogs: RDD[String] = ...
	val firstlogsWithErrors = lastYearslogs.filter(_.contains("ERROR")) .take(10) 
	
	The execution of filter is deferred until the take action is applied.
	Spark leverages this by analyzing and optimizing the chain of operations before
	executing it.
	Spark will not compute intermediate RDDs. Instead, as soon as 10 elements of the
	filtered RDD have been computed, firstLogsWi thErrors is done. At this point Spark
	stops working, saving time and space computing elements of the unused result of filter.   

	RDDs also support set-like operations, like union and intersection.
	Two-RDD transformations combine two RDDs are combined into one.
	union
	intersection
	substract
	cartesian
	
	RDDS also contain other important actions unrelated to resgular Scala
	collections, but which are useful when dealing with distributed data.
	takeSample
	takeOrdered
	saveAsTextFile
	saveAsSequenceFile
	
	
  */
}