package com.hjj.sparkcore

import org.apache.spark.rdd.RDD
import org.apache.spark.{Partitioner, SparkConf, SparkContext}

/**
  * @author soberhjj  2020/3/29 - 22:25
  *         自定义分区器
  */
object demo03 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("demo03")
    val sc = new SparkContext(conf)

    val rdd1: RDD[(Int, String)] = sc.parallelize(List((1,"a"),(2,"b"),(3,"c"),(4,"d"),(5,"e")))

    rdd1.partitionBy(new MyPartitioner(3)).saveAsTextFile("demo03")


  }

}

//声明分区器
class MyPartitioner(partitions:Int) extends Partitioner{
  override def numPartitions: Int = {
     partitions

  }

  override def getPartition(key: Any): Int = {
    1 //直接全都放进partition1
  }
}
