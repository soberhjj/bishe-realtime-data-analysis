package com.hjj.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;

import java.io.IOException;

/**
 * @author soberhjj  2020/4/23 - 10:55
 *
 * Hbase操作工具类 (使用单例模式封装)
 */
public class HbaseUtil {

    HBaseAdmin admin=null;
    Configuration configuration=null;

    //私有构造方法
    private HbaseUtil(){
        configuration=new Configuration();
        configuration.set("hbase.zookeeper.quorum","master:2181");
        configuration.set("hbase.rootdir","hdfs://master:9000/hbase");

        try {
            admin=new HBaseAdmin(configuration);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static HbaseUtil instance=null;

    public static synchronized HbaseUtil getInstance(){
        if (instance==null){
            instance=new HbaseUtil();
        }
        return instance;
    }

    /**
     * 根据表明获取到 HTable实例
     * @param tableName
     * @return
     */
    public HTable getHTable(String tableName){
        HTable table=null;

        try {
            table=new HTable(configuration,tableName);
        } catch (IOException e) {
            e.printStackTrace();
        }

        return table;
    }

    /**
     * 添加记录到hbase表
     * @param tableName
     * @param rowkey
     * @param cf
     * @param column
     * @param value
     */
    public void put(String tableName,String rowkey,String cf,String column,String value){
        HTable table=getHTable(tableName);

        Put put=new Put(rowkey.getBytes());
        put.add(cf.getBytes(),column.getBytes(),value.getBytes());

        try {
            table.put(put);
        } catch (IOException e) {
            e.printStackTrace();
        }

    }


//    public static void main(String[] args) {
//        HbaseUtil instance = HbaseUtil.getInstance();
//
//        instance.put("category_clickcount","20200423_donghua","info","click_count","2");
//
//    }



}
