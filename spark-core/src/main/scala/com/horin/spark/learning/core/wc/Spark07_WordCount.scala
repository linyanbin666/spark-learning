package com.horin.spark.learning.core.wc

import org.apache.spark.{SparkConf, SparkContext}

object Spark07_WordCount {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local").setAppName("word-count")
    val spark = SparkContext.getOrCreate(sparkConf)

    // countByValue
    spark.textFile("data")
        .flatMap(_.split(" "))
        .countByValue()
        .foreach(println)

    spark.stop()
  }

}
