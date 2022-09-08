package com.horin.spark.learning.core.wc

import org.apache.spark.{SparkConf, SparkContext}

object Spark03_WordCount {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local").setAppName("word-count")
    val spark = SparkContext.getOrCreate(sparkConf)

    // combineByKey，可设置初始值获取函数，可区分分区内和分区间计算逻辑
    val wordToCount = spark.textFile("data")
      .flatMap(_.split(" "))
      .map(word => (word, 1))
      .combineByKey(
        v => v,
        (x: Int, y: Int) => x + y,
        (x: Int, y: Int) => x + y
      )
    wordToCount.collect().foreach(println)

    spark.stop()
  }

}
