package com.horin.spark.learning.core.bc

import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

object BroadcastTest {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("bc")
    val spark = SparkContext.getOrCreate(sparkConf)

    val data = spark.makeRDD(List(
      ("a", 1), ("b", 2), ("c", 3)
    ))
    val map = mutable.Map[String, Int](
      ("a", 2), ("b", 3), ("c", 4)
    )
    // 使用广播变量，可以使共享变量在每个Executor中只存在一份，而不是在每个Task中存一份
    val bcMap = spark.broadcast(map)
    data.map {
      case (word, count) =>
        val i = bcMap.value.getOrElse(word, 0)
        (word, (count, i))
    }.collect().foreach(println)
    spark.stop()
  }

}
