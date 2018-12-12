package com.panCalka.spark

import org.apache.spark.SparkContext

object CustomerSpendTotal extends App{

  val spark =  new SparkContext("local[*]", "customerSpend")

  val text = spark.textFile("../customer-orders.csv")

  val pair = text
    .map(x => x.split(","))
    .map(x => (x(0), x(2).toFloat))
    .reduceByKey((x,y)=> x+y)
    .sortBy(_._2, false, 1)
    .foreach(x => println(x))


}
