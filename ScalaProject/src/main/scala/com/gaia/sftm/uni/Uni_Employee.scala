package com.gaia.sftm.uni

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

/**
  * @author michael
  * @create 2020-03-02 17:58
  */
object Uni_Employee {
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
      .appName("Uni_Employee")
//      .master("local[*]")
      .enableHiveSupport()
      .getOrCreate()

    import spark.implicits._

    val df_uni_employee: DataFrame = spark.sql(
      """
        |select
        |    id,
        |    emp_id,
        |    emp_code,
        |    emp_name,
        |    emp_sex,
        |    emp_mobile,
        |    emp_tel,
        |    emp_idcard,
        |    emp_birthday,
        |    emp_email,
        |    emp_addr,
        |    emp_qq,
        |    emp_weixin,
        |    emp_org_id,
        |    emp_org_code,
        |    waiqin365_org_id,
        |    emp_is_org_learder,
        |    emp_parent_id,
        |    parent_code,
        |    waiqin365_parent_id,
        |    emp_imsi_binding,
        |    emp_position,
        |    emp_job,
        |    emp_status,
        |    create_time,
        |    modify_time,
        |    call_date,
        |    bu,
        |    exts
        |from
        |(
        |    select row_number() over(partition by bu, id order by call_date desc) as uuid,* from ods_sftm_new.ods_org_emp_tmp
        |) as t1
        |where t1.uuid = 1
      """.stripMargin)

    df_uni_employee.write.mode(SaveMode.Overwrite).saveAsTable("ods_sftm_new.ods_org_emp")
    //  下面的repartition, 产生N个HDFS文件, 也可以按照列来产生...具体可以参考API文档.
    //    df_uni_store.repartition(1).write.mode(SaveMode.Overwrite).saveAsTable("ods_sftm_new.ods_cust_store")
    spark.stop()
  }
}
