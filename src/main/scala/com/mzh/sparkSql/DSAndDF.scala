package com.mzh.sparkSql


import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}


object DSAndDF {
  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder()
      .appName("Spark-Sql")
      .master("local[*]")
      .getOrCreate()


    val persons: Seq[Person] = Seq(Person(1,"zhangsan",20))
    import spark.implicits._
    val personDS: Dataset[Person] = persons.toDS()
    personDS.show()

    val personDF: DataFrame = personDS.toDF()

    personDF.show()

    val DFToDS: Dataset[Person] = personDF.as[Person]

    DFToDS.show()

    spark.stop()
  }
}
case class Person1 (id:Int,name:String,age:Int)
