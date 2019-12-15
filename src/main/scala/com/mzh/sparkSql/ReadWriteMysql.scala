package com.mzh.sparkSql

import java.util.Properties


import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}


object ReadWriteMysql {
  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder()
      .appName("Spark-Sql")
      .master("local")
      .getOrCreate()

    val driver = "com.mysql.jdbc.Driver"
    val host = "hdp1"
    val port = 3306
    val user = "bduser"
    val pwd = "bduser"
    val url = s"jdbc:mysql://${host}:${port}/bdtest"

    val properties = new Properties()
    properties.setProperty("user",user)
    properties.setProperty("password",pwd)

    val userDF: DataFrame = spark.read.jdbc(url,"user",properties)
    userDF.show()

    val user1DF: DataFrame = spark.read.format("jdbc")
      .option("url", url)
      .option("dbtable", "user1")
      .option("user", user)
      .option("password", pwd)
      .load()

    user1DF.show()

    val selectDF: DataFrame = userDF.select("name","age")
    selectDF.write.mode(SaveMode.Append).jdbc(url,"user1",properties)

    val select1DF: DataFrame = user1DF.select("name","age")
    select1DF.write.mode(SaveMode.Overwrite)
        .format("jdbc")
        .option("url", url)
        .option("dbtable", "user3")
        .option("user", user)
        .option("password", pwd)
        .save()



    spark.stop()
  }
}
