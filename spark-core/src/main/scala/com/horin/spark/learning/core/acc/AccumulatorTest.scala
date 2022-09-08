package com.horin.spark.learning.core.acc

import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

object AccumulatorTest {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Acc")
    val spark = SparkContext.getOrCreate(sparkConf)

    val data = spark.makeRDD(List(
      "Hello Scala", "Hello Spark", "Spark SQL", "Spark Streaming"
    ))
    val wcAccumulator = new WordCountAccumulator()
    spark.register(wcAccumulator)

    data.flatMap(_.split(" "))
      .foreach(word => {
        wcAccumulator.add(word)
      })

    println(wcAccumulator.value)

    spark.stop()
  }

  class WordCountAccumulator extends AccumulatorV2[String, mutable.Map[String, Long]] {

    private val wcMap = mutable.Map[String, Long]()

    override def isZero: Boolean = wcMap.isEmpty

    override def copy(): AccumulatorV2[String, mutable.Map[String, Long]] = new WordCountAccumulator

    override def reset(): Unit = wcMap.clear()

    override def add(v: String): Unit = {
      val newCount = wcMap.getOrElse(v, 0L) + 1
      wcMap.update(v, newCount)
    }

    override def merge(other: AccumulatorV2[String, mutable.Map[String, Long]]): Unit = {
      other.value.foreach{
        case (word, count) =>
          val newCount = wcMap.getOrElse(word, 0L) + count
          wcMap.update(word, newCount)
      }
    }

    override def value: mutable.Map[String, Long] = wcMap

  }

}
