package com.mzh.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

//map 算子
object OperJoin {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Make RDD")
    val sc = new SparkContext(conf)

    val listRDD1: RDD[(Int, String)] = sc.makeRDD(Array((1,"a"),(2,"b"),(3,"c"),(4,"d")))
    val listRDD2: RDD[(Int, Int)] = sc.makeRDD(Array((1,4),(2,5),(3,6)))

    val joinRDD: RDD[(Int, (String, Int))] = listRDD1.join(listRDD2)



    println(joinRDD.collect().mkString(" "))


    //val listRDD3: RDD[(Int, String)] = sc.makeRDD(Array((1,"a"),(2,"b"),(3,"c"),(4,"d")))
    val listRDD3: RDD[(Int, Int)] = sc.makeRDD(Array((1,4),(2,5),(3,6)),2)

    val joinRDD2: RDD[(Int, (String, Int))] = listRDD1.join(listRDD3)

    println(joinRDD2.collect().mkString(" "))



    sc.stop()
  }
}
