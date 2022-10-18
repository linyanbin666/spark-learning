package com.horin.spark.learning.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.{Encoder, Encoders, SparkSession, functions}

object UDAFTest {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .config(new SparkConf().setMaster("local[*]").setAppName("basic"))
      .getOrCreate()

    val df = spark.read.json("data/user_list.txt")
    df.createOrReplaceTempView("user")
    spark.udf.register("myAvg", functions.udaf(new MyAvg()))

    spark.sql("select myAvg(age) from user").show()

    spark.close()
  }

  case class Buff(var total: Long, var count: Long)

  class MyAvg extends Aggregator[Long, Buff, Long] {

    override def zero: Buff = Buff(0L, 0L)

    override def reduce(buff: Buff, in: Long): Buff = {
      buff.total += in
      buff.count += 1
      buff
    }

    override def merge(buff1: Buff, buff2: Buff): Buff = {
      buff1.total += buff2.total
      buff1.count += buff2.count
      buff1
    }

    override def finish(buff: Buff): Long = {
      if (buff.count == 0) {
        0
      } else {
        buff.total / buff.count
      }
    }

    override def bufferEncoder: Encoder[Buff] = Encoders.product

    override def outputEncoder: Encoder[Long] = Encoders.scalaLong

  }

}
