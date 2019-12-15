package com.mzh.sparkSql

import org.apache.spark.sql.{DataFrame, SparkSession}


object SparkSQL1 {
  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder()
      .appName("Spark-Sql")
      .master("local[*]")
      .getOrCreate()

    val df: DataFrame = spark.read.json("./input/user.json")

    df.show()

    df.printSchema()

    df.createOrReplaceTempView("student")

    spark.sql("select * from student").show()

    spark.sql("select age,count(*) from student group by age").show()


    //spark.newSession().sql("select * from student")
    df.createGlobalTempView("student")
    spark.newSession().sql("select * from global_temp.student").show()

    df.select("name").show()

    df.groupBy("age").count().show()

    spark.stop()
  }
}
