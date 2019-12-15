package com.mzh.sparkSql

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}


object ReadJson {
  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder()
      .appName("Spark-Sql")
      .master("local[*]")
      .getOrCreate()


    val df: DataFrame = spark.read.json("./input/user.json")

    val rdd: RDD[Row] = df.rdd

    println(rdd.collect().mkString(" "))

    val loadDF: DataFrame = spark.read.format("json").load("./input/user.json")
    loadDF.show()



    spark.stop()
  }
}
