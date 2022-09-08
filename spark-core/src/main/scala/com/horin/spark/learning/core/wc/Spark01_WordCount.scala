package com.horin.spark.learning.core.wc

import org.apache.spark.{SparkConf, SparkContext}

object Spark01_WordCount {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local").setAppName("word-count")
    val spark = SparkContext.getOrCreate(sparkConf)

    // foldByKey，可设置初始值
    val wordToCount = spark.textFile("data")
        .flatMap(_.split(" "))
        .map(word => (word, 1))
        .foldByKey(0)(_ + _)
    wordToCount.collect().foreach(println)

    spark.stop()
  }

}
