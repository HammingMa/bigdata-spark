package com.mzh.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

//map 算子
object OperFoldByKey {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Make RDD")
    val sc = new SparkContext(conf)

    val listRDD: RDD[(String, Int)] = sc.makeRDD(List(("a",3),("a",2),("c",4),("b",3),("c",6),("c",8)),2)

    listRDD.glom().collect().foreach(t => println(t.mkString(" ")))

    val foldByKeyRDD: RDD[(String, Int)] = listRDD.foldByKey(0)(_+_)

    foldByKeyRDD.glom().collect().foreach(t => println(t.mkString(" ")))

    val combineByKeyRDD: RDD[(String, Int)] = listRDD.combineByKey(x =>x,(x:Int,y:Int)=>x+y,(x:Int,y:Int)=>x+y)


    println(combineByKeyRDD.collect().mkString(" "))


    sc.stop()
  }
}
