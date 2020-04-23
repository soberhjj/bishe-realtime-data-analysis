package com.hjj.sparkcore

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author soberhjj  2020/3/29 - 14:53
  *
  *         测试groupBy算子
  *
  *         需求：创建一个RDD，按照元素模2的值进行分组
  */
object demo02 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("demo02")
    val sc = new SparkContext(conf)

    val rdd1: RDD[Int] = sc.makeRDD(1 to 10)

    rdd1.saveAsTextFile("demo01")

    val rdd2: RDD[(Int, Iterable[Int])] = rdd1.groupBy(elem=>elem%2)

    rdd2.saveAsTextFile("demo02")
  }
}
