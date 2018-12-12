package com.panCalka.spark

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._

/** Count up how many of each star rating exists in the MovieLens 100K data set. */
object RatingsCounter {
 
  def main(args: Array[String]) {
   
    Logger.getLogger("org").setLevel(Level.ERROR)
    val startTime = System.currentTimeMillis()

    val sc = new SparkContext("local[*]", "RatingsCounter")
   
    val lines = sc.textFile("../ml-100k/u.data")

    val ratings = lines.map(x => x.toString().split("\t")(2))
    
    val results = ratings.countByValue()

    val sortedResults = results.toSeq.sortBy(_._1)
    
    sortedResults.foreach(println)
    println(System.currentTimeMillis() - startTime)
  }
}
