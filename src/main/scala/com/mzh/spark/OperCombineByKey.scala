package com.mzh.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

//求平均值
object OperCombineByKey {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Make RDD")
    val sc = new SparkContext(conf)

    val listRDD: RDD[(String, Int)] = sc.makeRDD(Array(("a", 88), ("b", 95), ("a", 91), ("b", 93), ("a", 95), ("b", 98)),2)

    listRDD.glom().collect().foreach(t => println(t.mkString(" ")))

    val combineByKeyRDD: RDD[(String, (Int, Int))] = listRDD.combineByKey((_,1),(t:(Int,Int),v : Int)=>(t._1+v,t._2+1),(t1:(Int,Int),t2:(Int,Int))=>(t1._1+t2._1,t1._2+t2._2))


    combineByKeyRDD.glom().collect().foreach(t => println(t.mkString(" ")))

    val mapRDD: RDD[(String, Double)] = combineByKeyRDD.map {
      case (key, value) => (key, value._1 / value._2.toDouble)
    }

    println(mapRDD.collect().mkString(" "))




    sc.stop()
  }
}
