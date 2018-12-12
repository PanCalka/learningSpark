package com.panCalka.spark

import org.apache.spark.sql.SparkSession

object NestedJoinTest extends App {

  val sc = SparkSession.builder().master("local[*]").getOrCreate()
  val tuples = Seq(
    ("owner1", null, 0.5),
    ("owner1", "obj2", 0.2),
    ("owner2", "obj3   ", 0.1)
  )

  import sc.implicits._
  import org.apache.spark.sql.functions._

  val df1 = tuples.toDF("owner", "object", "score")

  case class Obj(value: (String, Boolean, Double))
  case class InnerObj(val1: String, val2: Boolean, val3: Double)

  val obj = Obj(null)
  val df2 = sc.sparkContext.parallelize(Seq(obj), 2).toDF()

  df2.show()

  val joinTables = df2.alias("d")
    .join(df1.alias("u"), col("u.object") === col("d.value._1"))

  joinTables.show()

}
