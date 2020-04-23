package com.hjj.streaming

import com.hjj.bean.{CategoryClickCount, Log}
import com.hjj.dao.CategoryClickCountDao
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils

import scala.collection.mutable.ListBuffer



/**
  * @author soberhjj  2020/4/20 - 12:03
  */
object KafkaInput {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[2]").setAppName("KafkaInput")
    val ssc = new StreamingContext(conf, Seconds(1))

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "master:9092,slave1:9092,slave2:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "spark",
      "auto.offset.reset" -> "latest"
    )

    val topics=Array("test")

    val stream = KafkaUtils.createDirectStream[String,String](ssc,PreferConsistent,  Subscribe[String, String](topics, kafkaParams))

    //数据清洗
    val logs=stream.map(record => record.value)
    val cleanData=logs.map(line=>{
      val infos=line.split("\t")
      Log(DateUtil.change(infos(0)),infos(1))
    })

//    cleanData.print()

    cleanData.map(x=>{
      //将数据转换成hbase中的rowkey
      (x.time.substring(0,8)+"_"+x.category,1)
    }).reduceByKey(_+_).foreachRDD(rdd=>{
      rdd.foreachPartition(partitionRecords=>{
        val list=new ListBuffer[CategoryClickCount]
        partitionRecords.foreach(pair=>{
          list.append(CategoryClickCount(pair._1,pair._2))
        })

        CategoryClickCountDao.save(list)
      })
    })


    ssc.start()
    ssc.awaitTermination()

  }

}
