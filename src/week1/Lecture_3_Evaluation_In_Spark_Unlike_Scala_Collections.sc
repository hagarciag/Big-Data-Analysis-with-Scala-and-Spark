package week1

object Lecture_3_Evaluation_In_Spark_Unlike_Scala_Collections {
  /*
  Let's start by recapping some major themes from previous sessions:
  - We learned the difference between transformations and actions.
  	* Transformations: Deferred/lazy
  	* Actions: Eager, kick of staged transformations.
  	
  - We learned that latency makes a big difference; too much latency
    wastes the time of the data analyst.
    * In-memory computation: Significantly lower latencies (several orders
      of magnitude!)
      
    By default, RDDs are recomputed each time you run an action on them.
    This can be expensive (in time) if you need to use a dataset more than
    once.
    
    Spark allows you to control what is cached in memory.
    
    There are many ways to configure how your data is persisted including in
    memoery, disk, both, and as regular Java objects or serialized...
    
    cache:
    Shorthand for using the default storage level, whuch is in memoery only as
    regular Java objects.
    
    persist:
    Persistance can be customized with this method. Pass the storage level
    you'd like as a parameter to persist.
    
    
  */
}