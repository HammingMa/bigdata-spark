package com.mzh.sparkSql

import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, DoubleType, LongType, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

object MyUDAF {
  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder()
      .appName("Spark-Sql")
      .master("local[*]")
      .getOrCreate()

    val df: DataFrame = spark.read.json("./input/user.json")

    //注册函数
    spark.udf.register("avgAge",new AvgAge)

    df.createOrReplaceTempView("user")

    spark.sql("select avgAge(age) from user").show()

  }
}

class AvgAge extends UserDefinedAggregateFunction{
  //输入参数的格式和类型
  override def inputSchema: StructType = {
    new StructType().add("age",LongType)
  }

  //缓冲器数据的格式和类型
  override def bufferSchema: StructType = {
    new StructType().add("sum",LongType).add("count",LongType)
  }

  //返回的类型
  override def dataType: DataType = {
    DoubleType
  }

  //函数是否稳定
  override def deterministic: Boolean = {
    true
  }

  //初始化缓冲区的数据
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0)=0L
    buffer(1)=0L
  }

  //根据查询结果更新缓冲区
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    buffer(0)=buffer.getLong(0)+input.getLong(0)
    buffer(1)=buffer.getLong(1)+1
  }

  //合并缓冲区
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1(0)=buffer1.getLong(0) + buffer2.getLong(0)
    buffer1(1)=buffer1.getLong(1) + buffer2.getLong(1)
  }

  //计算结果
  override def evaluate(buffer: Row): Double = {
    buffer.getLong(0).toDouble/buffer.getLong(1)
  }

}
