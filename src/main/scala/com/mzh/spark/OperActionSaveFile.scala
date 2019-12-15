package com.mzh.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object OperActionSaveFile {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local").setAppName("action")
    val sc = new SparkContext(conf)

    val listRDD: RDD[(Int, String)] = sc.makeRDD(List((1,"a"),(2,"b"),(3,"c"),(4,"d"),(5,"f"),(6,"g"),(7,"h")))

    listRDD.saveAsTextFile("./outputText")
    listRDD.saveAsSequenceFile("./outputSeq")
    listRDD.saveAsObjectFile("./outputObj")
  }
}
