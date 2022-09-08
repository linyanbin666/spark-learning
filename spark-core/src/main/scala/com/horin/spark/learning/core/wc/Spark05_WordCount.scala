package com.horin.spark.learning.core.wc

import org.apache.spark.{SparkConf, SparkContext}

object Spark05_WordCount {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local").setAppName("word-count")
    val spark = SparkContext.getOrCreate(sparkConf)

    // groupByKey
    val wordToCount = spark.textFile("data")
        .flatMap(_.split(" "))
        .map(word => (word, 1))
        .groupByKey()
        .mapValues(_.size)
    wordToCount.collect().foreach(println)

    spark.stop()
  }

}
