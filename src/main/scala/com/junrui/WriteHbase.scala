package com.junrui

import org.apache.hadoop.hbase.client.{Put, Result}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat
import org.apache.hadoop.hbase.{HBaseConfiguration, HConstants}
import org.apache.hadoop.hbase.spark.HBaseContext
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.{SparkConf, SparkContext}

object WriteHbase {
  def main(args: Array[String]): Unit = {
    //建立SC
    val conf = new SparkConf().setMaster("local[*]").setAppName("asdfasdf")
    val sc = new SparkContext(conf)

    //HBaseConfiguration
    val hbaseConf = HBaseConfiguration.create()
    //给HbaseConfiguration配置zookeeper的地址
    hbaseConf.set(HConstants.ZOOKEEPER_QUORUM,"team01master,team01slave01,team01slave02")
    hbaseConf.set(HConstants.ZOOKEEPER_CLIENT_PORT,"2181")
    //表名
    hbaseConf.set(TableOutputFormat.OUTPUT_TABLE,"table_spark")
    //构造Job
    val job = Job.getInstance(hbaseConf)
    //key类型
    job.setOutputKeyClass(classOf[ImmutableBytesWritable])
    //value类型
    job.setOutputValueClass(classOf[Result])
    //格式类型
    job.setOutputFormatClass(classOf[TableOutputFormat[ImmutableBytesWritable]])

    //写入数据到hbaes
    val hbaesResult = sc.makeRDD(100 to 1000).map(x => {
      //put方法写入，rowkey
      //hash设计rowkey
      val str = RowKeyUtil.getRegNo2("rowkey001"+x,3)
      println(str)
      val put = new Put(Bytes.toBytes(str+"rowkey001" + x))
      //列族名cf01，字段名name，内容x
      put.add(Bytes.toBytes("cf01"), Bytes.toBytes("name"), Bytes.toBytes(x))

      (new ImmutableBytesWritable, put)
    })
    hbaesResult.saveAsNewAPIHadoopDataset(job.getConfiguration)
  }
}
