package com.panCalka.spark

import java.nio.charset.CodingErrorAction

import org.apache.log4j._
import org.apache.spark.SparkContext

import scala.io.{Codec, Source}

object MostPopSuperhero extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)


  def superHeroNames()= {
    implicit val codec = Codec("UTF-8")
    codec.onMalformedInput(CodingErrorAction.REPLACE)
    codec.onUnmappableCharacter(CodingErrorAction.REPLACE)

    var names: Map[Int, String] = Map()

    val lines = Source.fromFile("../Marvel-names.txt").getLines()
    for (line <- lines) {
      val fields = line.split('\"')
      if(fields.length >1) {
        names += (fields(0).trim.toInt -> fields(1))
      }
    }
    names
  }

  def countCoOccurences(line: String) = {
    var elements = line.split("\\s+")
    ( elements(0).toInt, elements.length - 1 )
  }

  val spark = new SparkContext("local[*]", "superhero_names")
  val superNames = spark.broadcast(superHeroNames)
  val graph = spark.textFile("../Marvel-graph.txt")
    .map(countCoOccurences)
    .reduceByKey((x, y) => x + y)
    .map(x => (x._2, x._1))
    .max()


  println(superNames.value(graph._2) )
}
