package com.mzh.spark

import java.util

import org.apache.spark.rdd.RDD
import org.apache.spark.util.{AccumulatorV2, LongAccumulator}
import org.apache.spark.{SparkConf, SparkContext}

object MyAccumulator {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("acc").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val listRDD: RDD[String] = sc.makeRDD(List("hadoop","hbase","hive","spark","scala"),2)

    val accumulator = new WordAccumulator()

    sc.register(accumulator)

    listRDD.foreach{
      (word) =>{
        accumulator.add(word)
      }
    }

    println("sum = "+accumulator.value)

  }
}

class WordAccumulator extends AccumulatorV2[String, util.ArrayList[String]]{

  private val words = new util.ArrayList[String]()

  override def isZero: Boolean = {
    words.isEmpty
  }

  override def copy(): AccumulatorV2[String, util.ArrayList[String]] = {
    new WordAccumulator()
  }


  override def reset(): Unit = {
    words.clear()
  }


  override def add(v: String): Unit = {
    if(v.contains("h")){
      words.add(v)
    }
  }


  override def merge(other: AccumulatorV2[String, util.ArrayList[String]]): Unit = {
    words.addAll(other.value)
  }

  override def value: util.ArrayList[String] = {
    words
  }
}
