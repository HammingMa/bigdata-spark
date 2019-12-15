package com.mzh.sparkSql

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}


object WriteJson {
  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder()
      .appName("Spark-Sql")
      .master("local")
      .getOrCreate()

    val sc: SparkContext = spark.sparkContext

    val listRDD: RDD[(Int, String, Int)] = sc.makeRDD(List((1,"zhangsan",20),(2,"lisi",30),(3,"wangwu",40)))

    import spark.implicits._

    val df: DataFrame = listRDD.toDF("id","name","age")
    df.show()

    df.write.format("json").save("./output")
    df.write.mode(SaveMode.Append).json("./output")


    spark.stop()
  }
}
