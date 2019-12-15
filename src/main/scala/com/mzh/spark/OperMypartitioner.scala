package com.mzh.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, Partitioner, SparkConf, SparkContext}

//map 算子
object OperMypartitioner {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Make RDD")
    val sc = new SparkContext(conf)

    val listRDD: RDD[(String, Int)] = sc.makeRDD(Array(("a",1),("b",2),("c",3),("d",4)))

    val partitionByRDD: RDD[(String, Int)] = listRDD.partitionBy(new Mypartitioner(2))

    println(partitionByRDD.collect().mkString(" "))

    partitionByRDD.saveAsTextFile("./output")

    sc.stop()
  }
}

class Mypartitioner(pn : Int) extends Partitioner{
  override def numPartitions: Int = pn

  override def getPartition(key: Any): Int = {
    if(key.toString() == "a"){
      0
    }else{
      1
    }
  }

}
