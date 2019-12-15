package com.mzh.sparkSql



import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}


object RDDToDataFrame {
  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder()
      .appName("RDD to DataFrame")
      .master("local[*]")
      .getOrCreate()

    val sc: SparkContext = spark.sparkContext

    val listRDD: RDD[(Int, String, Int)] = sc.makeRDD(List((1,"xiaoqi",12),(2,"zhangsan",40),(3,"wangwu",30),(4,"lisi",19),(5,"zhaoliu",18)))
    import spark.implicits._
    val df: DataFrame = listRDD.toDF("id","name","age")

    df.show()


    spark.stop()
  }
}
