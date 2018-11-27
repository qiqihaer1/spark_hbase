package com.junrui

import org.apache.hadoop.hbase.client.{Result, Scan}
import org.apache.hadoop.hbase.spark.HBaseContext
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, HConstants, TableName}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}


object ReadHbase  {
  def main(args: Array[String]): Unit = {
    //建立SC
    val conf = new SparkConf().setMaster("local[*]").setAppName("asdfasdf")
    val sc = new SparkContext(conf)

    //HBaseConfiguration
    val hbaseConf = HBaseConfiguration.create()
    //给HbaseConfiguration配置zookeeper的地址
    hbaseConf.set(HConstants.ZOOKEEPER_QUORUM,"team01master,team01slave01,team01slave02")
    //构造出HBaseContext
    val hbaseContext = new HBaseContext(sc,hbaseConf)
    //读取方法scan
    val scan=new Scan()
    scan.setCaching(10000)
    val hbaesRdd = hbaseContext.hbaseRDD(TableName.valueOf("table_spark"),scan)
    val unit: RDD[Result] = hbaesRdd.map(_._2)
    unit.foreach(x=>{
      val bytes: Array[Byte] = x.value()
      val str = Bytes.toString(bytes)
      println(str)
    })
  }

}
