package com.mzh.sparkSql

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}


object RDDToDFAsCaseClass {
  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder()
      .appName("RDD to DataFrame")
      .master("local[*]")
      .getOrCreate()

    val sc: SparkContext = spark.sparkContext

    val listRDD: RDD[(Int, String, Int)] = sc.makeRDD(List((1,"xiaoqi",12),(2,"zhangsan",40),(3,"wangwu",30),(4,"lisi",19),(5,"zhaoliu",18)))


    val personRDD: RDD[Person] = listRDD.map{t=> Person(t._1,t._2,t._3)}

    println(personRDD.collect().mkString(" "))

    import spark.implicits._

    val personDF: DataFrame = personRDD.toDF()



    personDF.show()


    spark.stop()
  }
}
case class Person3 (id:Int,name:String,age:Int)
