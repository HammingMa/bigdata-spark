package com.mzh.spark

import java.sql.{Connection, DriverManager, ResultSet}

import org.apache.spark.rdd.JdbcRDD
import org.apache.spark.{SparkConf, SparkContext}

object ReadMysql {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("read mysql").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val driver = "com.mysql.jdbc.Driver"
    val host = "hdp1"
    val port = 3306
    val user = "bduser"
    val pwd = "bduser"
    val url = s"jdbc:mysql://${host}:${port}/bdtest"
    val sql = "select name,age from bdtest.user where id>=? and id<=?"


    val mysqlRDD: JdbcRDD[Any] = new JdbcRDD(sc,
      () => {
        Class.forName(driver)
        DriverManager.getConnection(url, user, pwd)
      },
      sql,
      1,
      3,
      2,
      (rs) => {
        println(rs.getString("name") + " " + rs.getInt("age"))
      })

    mysqlRDD.collect()

  }
}
