package com.mzh.spark

import java.sql.DriverManager

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.{Cell, CellUtil, HBaseConfiguration}
import org.apache.hadoop.hbase.client.{Put, Result}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object ReadHbase {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("read mysql").setMaster("local[*]")
    val sc = new SparkContext(conf)


    val hconf: Configuration = HBaseConfiguration.create()
    hconf.set(TableInputFormat.INPUT_TABLE,"student")

    val resRDD: RDD[(ImmutableBytesWritable, Result)] = sc.newAPIHadoopRDD(hconf,
      classOf[TableInputFormat],
      classOf[ImmutableBytesWritable],
      classOf[Result])

    resRDD.foreach{
      case (rowkey,result)=>{
        val cells: Array[Cell] = result.rawCells()
        println(Bytes.toString(rowkey.get()))
        for (cell <- cells) {
          println(Bytes.toString(CellUtil.cloneValue(cell)))
        }
      }
    }

    val listRDD: RDD[(String, String)] = sc.makeRDD(List(("1002","lisi"),("1004","王五"),("1005","xiaozhao"),("1006","小王"),("1007","小刘")))

    val putRDD: RDD[(ImmutableBytesWritable, Put)] = listRDD.map {
      case (rowkey, name) => {
        val put = new Put(Bytes.toBytes(rowkey))

        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("name"), Bytes.toBytes(name))

        (new ImmutableBytesWritable(Bytes.toBytes(rowkey)), put)

      }
    }


    val jobConf = new JobConf(hconf)
    jobConf.setOutputFormat(classOf[TableOutputFormat])
    jobConf.set(TableOutputFormat.OUTPUT_TABLE,"student")
    putRDD.saveAsHadoopDataset(jobConf)

    resRDD.foreach{
      case (rowkey,result)=>{
        val cells: Array[Cell] = result.rawCells()
        println(Bytes.toString(rowkey.get()))
        for (cell <- cells) {
          println(Bytes.toString(CellUtil.cloneValue(cell)))
        }
      }
    }


  }
}
