package com.mzh.sparkSql




import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.{DataFrame, Dataset, Encoder, Encoders, Row, SparkSession, TypedColumn}

object MyUDAFClass {
  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder()
      .appName("Spark-Sql")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    val df: DataFrame = spark.read.json("./input/user.json")

    val userDS: Dataset[User] = df.as[User]

    //初始化udaf 对象
    val getAvgAge = new GetAvgAge()

    // 查询函数转化成列
    val avgAge: TypedColumn[User, Double] = getAvgAge.toColumn.name("avg_age")


    //使用查询列
    userDS.select(avgAge).show()

  }
}

//输入的类
case class User (name:String,age:Long)
//缓冲区的类
case class BuffAvg(var sum:Long,var count:Long)



class GetAvgAge extends Aggregator[User,BuffAvg,Double] {
  //初始化缓冲区
  override def zero: BuffAvg = {
    new BuffAvg(0,0)
  }

  //聚合数据
  override def reduce(b: BuffAvg, a: User): BuffAvg = {
    b.sum+=a.age
    b.count+=1
    return b
  }

  //合并缓冲区
  override def merge(b1: BuffAvg, b2: BuffAvg): BuffAvg = {
    b1.sum+=b2.sum
    b1.count+=b2.count
    return b1
  }

  //最终结果
  override def finish(reduction: BuffAvg): Double = {
    reduction.sum/reduction.count.toDouble
  }

  override def bufferEncoder: Encoder[BuffAvg] = Encoders.product

  override def outputEncoder: Encoder[Double] = Encoders.scalaDouble
}


