package com.horin.spark.learning.core.wc

import org.apache.spark.{SparkConf, SparkContext}

object Spark06_WordCount {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local").setAppName("word-count")
    val spark = SparkContext.getOrCreate(sparkConf)

    // countByKey
    spark.textFile("data")
        .flatMap(_.split(" "))
        .map(word => (word, 1))
        .countByKey()
        .foreach(println)

    spark.stop()
  }

}
