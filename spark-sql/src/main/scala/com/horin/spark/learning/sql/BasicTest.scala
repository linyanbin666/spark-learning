package com.horin.spark.learning.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object BasicTest {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .config(new SparkConf().setMaster("local[*]").setAppName("basic"))
      .getOrCreate()
    import spark.implicits._

    val df = spark.read.json("data/user_list.txt")
    df.createOrReplaceTempView("user")
    spark.sql("select name from user").show()

    // DataFrame <=> DataSet
    val ds = df.as[User]
    ds.show()
    ds.toDF().show()

    // DataFrame <=> RDD
    val rdd = spark.sparkContext.makeRDD(List(("Horin", 25), ("Jack", 24)))
    val rddDf = rdd.toDF("name", "age")
    rddDf.show()
    rddDf.rdd.foreach(println)

    // DataSet <=> RDD
    val userRdd = rdd.map {
      case (name, age) => User(name, age)
    }
    val userDs = userRdd.toDS()
    userDs.show()
    userDs.rdd.foreach(println)

    spark.close()
  }

  case class User(name: String, age: Long)

}
