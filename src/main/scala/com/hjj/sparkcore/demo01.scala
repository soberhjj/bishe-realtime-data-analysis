package com.hjj.sparkcore

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author soberhjj  2020/3/29 - 11:32
  *
  *         测试glom算子
  */
object demo01 {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("demo01")

    val sc = new SparkContext(sparkConf)

    val rdd1: RDD[Int] = sc.makeRDD(1 to 16,4)

    val rdd2: RDD[Array[Int]] = rdd1.glom()

    rdd2.collect().foreach(array=>{
      println(array.mkString(","))
    })

  }

}
