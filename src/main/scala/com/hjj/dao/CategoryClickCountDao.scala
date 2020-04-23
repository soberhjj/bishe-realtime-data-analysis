package com.hjj.dao

import scala.collection.mutable.ListBuffer
import com.hjj.bean.CategoryClickCount
import com.hjj.utils.HbaseUtil
import org.apache.hadoop.hbase.client.Get
import org.apache.hadoop.hbase.util.Bytes

/**
  * @author soberhjj  2020/4/23 - 10:41
  *
  *         访问 hbase
  */
object CategoryClickCountDao {

  val table_Name="category_clickcount"
  val cf="info"
  val qualifer="click_count"


  /**
    * 保存数据到hbase
    * @param list  即 CategoryClickCount集合
    */
  def save(list: ListBuffer[CategoryClickCount]):Unit={
    val table=HbaseUtil.getInstance().getHTable(table_Name)

    for(ele <- list){
      table.incrementColumnValue(ele.day_category.getBytes(),
        cf.getBytes(),
        qualifer.getBytes(),
        ele.click_count)
    }

  }


  /**
    * 从hbase中获取数据
    * @param day_category 即 hbase中的rowkeyo
    * @return
    */
  def count(day_category:String):Long={
    val table=HbaseUtil.getInstance().getHTable(table_Name)

    val get=new Get(day_category.getBytes())
    val value=table.get(get).getValue(cf.getBytes(),qualifer.getBytes())

    if(value==null)
      0L
    else
      Bytes.toLong(value)
  }

//  def main(args: Array[String]): Unit = {
////    val list=new ListBuffer[CategoryClickCount]
////    list.append(CategoryClickCount("20200430_life",10))
////    list.append(CategoryClickCount("20200430_donghua",5))
////
////    save(list)
//
////    println(count("20200430_donghua"))
//
//  }

}
