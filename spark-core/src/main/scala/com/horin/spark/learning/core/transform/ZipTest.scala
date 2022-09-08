package com.horin.spark.learning.core.transform

import org.apache.spark.{SparkConf, SparkContext}

object ZipTest {

  def main(args: Array[String]): Unit = {
    val spark = SparkContext.getOrCreate(new SparkConf().setMaster("local[*]").setAppName("zip-test"))

    val rdd1 = spark.makeRDD(List(1, 2, 3, 4), 2)
    val rdd2 = spark.makeRDD(List(7, 8, 9, 10), 2)

    rdd1.zip(rdd2).collect().foreach(println)

    spark.stop()
  }

}
