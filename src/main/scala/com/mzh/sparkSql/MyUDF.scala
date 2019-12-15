package com.mzh.sparkSql

import org.apache.spark.sql.{DataFrame, SparkSession}

object MyUDF {
  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder()
      .appName("Spark-Sql")
      .master("local[*]")
      .getOrCreate()

    val df: DataFrame = spark.read.json("./input/user.json")

    spark.udf.register("addName",(name:String)=>{"name:"+name})

    df.createOrReplaceTempView("user")

    spark.sql("select addName(name),age from user").show()

  }
}
