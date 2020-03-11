package com.gaia.sftm.uni

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

/**
  * @author michael
  * @create 2020-03-02 17:45
  */
object Uni_Depart {
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
      .appName("Uni_DepartMent")
//      .master("local[*]")
      .enableHiveSupport()
      .getOrCreate()

    import spark.implicits._

    val df_uni_department: DataFrame = spark.sql(
      """
        |select
        |    id,
        |    org_id,
        |    org_code,
        |    org_name,
        |    org_parent_id,
        |    parent_code,
        |    parent_name,
        |    waiqin365_parent_id,
        |    full_ids,
        |    full_codes,
        |    full_names,
        |    org_sequence,
        |    org_status,
        |    create_time,
        |    modify_time,
        |    call_date,
        |    bu
        |from
        |(
        |    select row_number() over(partition by bu,id order by call_date desc) as uuid, * from ods_sftm_new.ods_org_dep_tmp
        |) as t1
        |where t1.uuid = 1
      """.stripMargin)

    df_uni_department.write.mode(SaveMode.Overwrite).saveAsTable("ods_sftm_new.ods_org_dep")
    //  下面的repartition, 产生N个HDFS文件, 也可以按照列来产生...具体可以参考API文档.
    //    df_uni_store.repartition(1).write.mode(SaveMode.Overwrite).saveAsTable("ods_sftm_new.ods_cust_store")
    spark.stop()
  }
}
