package com.mzh.sparkSql

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}


object CreateDataSet {
  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder()
      .appName("Spark-Sql")
      .master("local[*]")
      .getOrCreate()


    val persons: Seq[Person] = Seq(Person(1,"zhangsan",20))
    import spark.implicits._
    val personDS: Dataset[Person] = persons.toDS()

    personDS.show()
  }
}
case class Person (id:Int,name:String,age:Int)
