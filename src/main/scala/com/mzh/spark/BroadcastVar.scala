package com.mzh.spark

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object BroadcastVar {
  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Cache RDD")
    val sc: SparkContext = new SparkContext(conf)

    val listRDD: RDD[(Int, String)] = sc.makeRDD(List((1,"zhangsan"),(2,"lisi"),(3,"wangwu"),(4,"zhaoliu"),(5,"sunqi"),(6,"wangyi")),2)
    val list = List((1,19),(2,18),(3,16),(4,20),(5,21),(6,22))

    //注册刚播变量
    val bc: Broadcast[List[(Int, Int)]] = sc.broadcast(list)

    val mapRDD: RDD[(Int, String, Int)] = listRDD.map {
      case (id, name) => {
        var age: Int = 0
        for ((id1, age1) <- bc.value) {
          if (id == id1) {
            age = age1
          }
        }
        (id, name, age)
      }
    }

    println(mapRDD.collect().mkString(" "))



  }
}
