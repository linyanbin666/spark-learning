package com.horin.spark.learning.core.wc

import org.apache.spark.{SparkConf, SparkContext}

object Spark02_WordCount {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local").setAppName("word-count")
    val spark = SparkContext.getOrCreate(sparkConf)

    // aggregateByKey，可设置初始值，可区分分区内和分区间计算逻辑
    val wordToCount = spark.textFile("data")
        .flatMap(_.split(" "))
        .map(word => (word, 1))
        .aggregateByKey(0)(_ + _, _ + _)
    wordToCount.collect().foreach(println)

    spark.stop()
  }

}
