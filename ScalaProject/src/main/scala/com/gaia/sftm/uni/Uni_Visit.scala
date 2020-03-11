package com.gaia.sftm.uni

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

/**
  * @author michael
  * @create 2020-02-25 15:06
  */
object Uni_Visit {

  def main(args: Array[String]): Unit = {

    System.setProperty("HADOOP_USER_NAME", "hadoop")
//    下面的代码本意是想1
//    System.setProperty("user.name", "root")

    val conf = new SparkConf
    conf.set("spark.sql.adaptive.enabled", "true")
    conf.set("spark.sql.adaptive.shuffle.targetPostShuffleInputSize", "128M")
    conf.set("spark.sql.adaptive.join.enabled", "true")
    conf.set("spark.sql.autoBroadcastJoinThreshold", "20971520")

    val spark = SparkSession
      .builder()
      .config(conf)
      .appName("Uni_Visit")
      //      .master("local[*]")
      .enableHiveSupport()
      .getOrCreate()

    import spark.implicits._

    var df_uni_visit:DataFrame = spark.sql(
      """
        |select
        |    id,
        |    status,
        |    creator_id,
        |    creator_name,
        |    creator_sourcecode,
        |    create_time,
        |    modifyier_id,
        |    modifyier_name,
        |    modifyier_sourcecode,
        |    modify_time,
        |    visitor,
        |    visitor_name,
        |    visitor_sourcecode,
        |    visit_type,
        |    flow_type,
        |    plan_source,
        |    approval_status,
        |    customer,
        |    customer_name,
        |    customer_sourcecode,
        |    visit_date,
        |    visit_time,
        |    sequence,
        |    visit_status,
        |    flow_set_id,
        |    flow_set_name,
        |    missed_type,
        |    missed_type_name,
        |    missed_regtime,
        |    missed_reason,
        |    missed_picture_path,
        |    missed_lla,
        |    is_finished,
        |    plan_content,
        |    arrive_time,
        |    arrive_lla,
        |    arrive_rg_type,
        |    arrive_pos_offset,
        |    arrive_picture_path,
        |    arrive_remarks,
        |    leave_time,
        |    leave_lla,
        |    leave_rg_type,
        |    leave_pos_offset,
        |    leave_picture_path,
        |    leave_remarks,
        |    time_consume,
        |    summary,
        |    summary_pictures,
        |    summary_time,
        |    del_person,
        |    del_person_name,
        |    del_time,
        |    call_date,
        |    bu
        |from
        |(
        |    select row_number() over(partition by bu,id order by call_date desc) as uuid, * from ods_sftm_new.ods_visit_tmp
        |) as t1
        |where t1.uuid = 1
      """.stripMargin)

    df_uni_visit.write.mode(SaveMode.Overwrite).saveAsTable("ods_sftm_new.ods_visit")
    //  下面的repartition, 产生N个HDFS文件, 也可以按照列来产生...具体可以参考API文档.
    //    df_uni_store.repartition(1).write.mode(SaveMode.Overwrite).saveAsTable("ods_sftm_new.ods_cust_store")
    spark.stop()
  }

}
