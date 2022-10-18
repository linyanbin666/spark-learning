package com.horin.spark.learning.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object UDFTest {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .config(new SparkConf().setMaster("local[*]").setAppName("basic"))
      .getOrCreate()

    val df = spark.read.json("data/user_list.txt")
    df.createOrReplaceTempView("user")
    spark.udf.register("common_prefix", (name: String) => "prefix_" + name)
    spark.sql("select common_prefix(name) from user").show()

    spark.close()
  }

}
