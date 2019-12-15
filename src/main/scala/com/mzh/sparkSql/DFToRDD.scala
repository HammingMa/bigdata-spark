package com.mzh.sparkSql

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}


object DFToRDD {
  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder()
      .appName("Spark-Sql")
      .master("local[*]")
      .getOrCreate()


    val df: DataFrame = spark.read.json("./input/user.json")

    val rdd: RDD[Row] = df.rdd

    println(rdd.collect().mkString(" "))

    spark.stop()
  }
}
