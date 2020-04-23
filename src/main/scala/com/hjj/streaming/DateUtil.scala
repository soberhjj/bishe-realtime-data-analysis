package com.hjj.streaming

import java.util.Date

import org.apache.commons.lang3.time.FastDateFormat


/**
  * @author soberhjj  2020/4/23 - 14:14
  */
object DateUtil {
  val YYYYMMDDHHMMSS_FORMAT=FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss")
  val TARGET_FORMAT=FastDateFormat.getInstance("yyyyMMddHHmmss")

  def getTime(time:String)={
    YYYYMMDDHHMMSS_FORMAT.parse(time).getTime
  }

  def change(time:String)={
    TARGET_FORMAT.format(new Date(getTime(time)))
  }

  def main(args: Array[String]): Unit = {
    println(change("2020-04-23 13:52:59"))
  }

}
