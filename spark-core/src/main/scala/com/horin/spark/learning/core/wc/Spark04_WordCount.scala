package com.horin.spark.learning.core.wc

import org.apache.spark.{SparkConf, SparkContext}

object Spark04_WordCount {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local").setAppName("word-count")
    val spark = SparkContext.getOrCreate(sparkConf)

    // groupBy
    val wordToCount = spark.textFile("data")
        .flatMap(_.split(" "))
        .groupBy(word => word)
        .mapValues(_.size)
    wordToCount.collect().foreach(println)

    spark.stop()
  }

}
