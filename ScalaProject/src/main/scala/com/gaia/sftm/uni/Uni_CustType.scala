package com.gaia.sftm.uni

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

/**
  * @author michael
  * @create 2020-03-02 17:13
  */
object Uni_CustType {
  def main(args: Array[String]): Unit = {

    System.setProperty("HADOOP_USER_NAME", "hadoop")
    System.setProperty("user.name", "root")

    val conf = new SparkConf
    conf.set("spark.sql.adaptive.enabled", "true")
    conf.set("spark.sql.adaptive.shuffle.targetPostShuffleInputSize", "128M")
    conf.set("spark.sql.adaptive.join.enabled", "true")
    conf.set("spark.sql.autoBroadcastJoinThreshold", "20971520")

    val spark = SparkSession
      .builder()
      .config(conf)
      .appName("Uni_CustType")
//      .master("local[*]")
      .enableHiveSupport()
      .getOrCreate()

    import spark.implicits._

    val df_uni_custtype: DataFrame = spark.sql(
      """
        |select
        |    type_id,
        |    type_code,
        |    type_name,
        |    type_parent_id,
        |    type_sequence,
        |    type_status,
        |    type_image,
        |    type_remarks,
        |    create_date,
        |    modify_date,
        |    trade_type,
        |    id,
        |    parent_id,
        |    call_date,
        |    bu
        |from
        |(
        |    select row_number() over(partition by id order by call_date desc) as uuid,* from ods_sftm_new.ods_cust_type_tmp
        |) as t1
        |where t1.uuid = 1
      """.stripMargin)

    df_uni_custtype.write.mode(SaveMode.Overwrite).saveAsTable("ods_sftm_new.ods_cust_type")
    //  下面的repartition, 产生N个HDFS文件, 也可以按照列来产生...具体可以参考API文档.
    //    df_uni_store.repartition(1).write.mode(SaveMode.Overwrite).saveAsTable("ods_sftm_new.ods_cust_store")
    spark.stop()
  }
}
