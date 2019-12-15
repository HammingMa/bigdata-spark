package com.mzh.spark

import java.sql.{Connection, DriverManager, PreparedStatement}

import org.apache.spark.rdd.{JdbcRDD, RDD}
import org.apache.spark.{SparkConf, SparkContext}

object WriteMysql {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("read mysql").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val driver = "com.mysql.jdbc.Driver"
    val host = "hdp1"
    val port = 3306
    val user = "bduser"
    val pwd = "bduser"
    val url = s"jdbc:mysql://${host}:${port}/bdtest"
    val sql = "insert into user1 (name,age) values(?,?)"


    val listRDD: RDD[(String, Int)] = sc.makeRDD(List(("lisi",90),("zhangsan",20),("zhangliu",30),("wangwu",10)))

    /*
    listRDD.foreach{
    case (name,age) =>{
      Class.forName(driver)
      val conn: Connection = DriverManager.getConnection(url, user, pwd)
      val statement: PreparedStatement = conn.prepareStatement(sql)
      statement.setString(1,name)
      statement.setInt(2,age)
      statement.executeUpdate()

      statement.close()
      conn.close()
    }}
     */

    listRDD.foreachPartition{
      (datas)=>{
        Class.forName(driver)
        val conn: Connection = DriverManager.getConnection(url, user, pwd)
        val statement: PreparedStatement = conn.prepareStatement(sql)
        datas.foreach{
          case (name,age)=>{
            statement.setString(1,name)
            statement.setInt(2,age)
            statement.executeUpdate()
          }
        }
        statement.close()
        conn.close()

      }
    }

    sc.stop()

  }
}
