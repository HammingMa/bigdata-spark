package com.mzh.sparkStreaming

object ScalaSliding {
  def main(args: Array[String]): Unit = {
    val list = List(1,2,3,4,5,6)

    //滑动
    //val ints: Iterator[List[Int]] = list.sliding(2)
    //val ints: Iterator[List[Int]] = list.sliding(3)
    val ints: Iterator[List[Int]] = list.sliding(3,3)

    for (arr <- ints) {
      println(arr.mkString(" "))
    }
  }
}
