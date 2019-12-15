package com.mzh.sparkSql

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}


object DSToRDD {
  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder()
      .appName("Spark-Sql")
      .master("local[*]")
      .getOrCreate()


    val persons: Seq[Person] = Seq(Person(1,"zhangsan",20))
    import spark.implicits._
    val personDS: Dataset[Person] = persons.toDS()
    personDS.show()

    val personRDD: RDD[Person] = personDS.rdd
    println(personRDD.collect().mkString(" "))
  }
}
case class Person2 (id:Int,name:String,age:Int)
