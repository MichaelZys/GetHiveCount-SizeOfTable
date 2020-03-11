package com.gaia.sftm.uni

import org.apache.spark.sql.{DataFrame, SparkSession, SaveMode}
import org.apache.spark.SparkConf

/**
  * @author michael
  * @create 2020-02-24 15:15
  */
object Uni_Store {

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
      .appName("Uni_Store")
//      .master("local[*]")
      .enableHiveSupport()
      .getOrCreate()

    import spark.implicits._

    var df_uni_store:DataFrame = spark.sql(
      """
        |select
        |    id,
        |    store_id,
        |    store_name,
        |    store_code,
        |    store_manager,
        |    store_manager_waiqin365_id,
        |    store_type,
        |    store_type_id,
        |    store_level_id,
        |    store_level,
        |    store_dept_waiqin365_id,
        |    store_dept_id,
        |    store_dept_name,
        |    store_district,
        |    store_mss_province,
        |    store_mss_city,
        |    store_mss_area,
        |    store_mss_street,
        |    store_addr,
        |    store_cooperate_status_id,
        |    store_cooperate_status,
        |    store_ka_sys,
        |    store_tel,
        |    store_fax,
        |    store_post,
        |    store_remarks,
        |    tradingarea_big,
        |    tradingarea,
        |    tradingarea_level_code,
        |    tradingarea_level_name,
        |    store_district_id,
        |    store_district_create_time,
        |    store_district_modify_time,
        |    store_district_creator_name,
        |    store_district_modifyier_name,
        |    store_district_status,
        |    store_rel_level_id,
        |    store_approval_status,
        |    store_status,
        |    create_time,
        |    call_date,
        |    bu,
        |    linkmans,
        |    deliverys,
        |    exts,
        |    dealers,
        |    lc_id,
        |    lc_cm_id,
        |    lc_cm_code,
        |    lc_cm_name,
        |    lc_location_a,
        |    lc_location_c,
        |    lc_mss_province,
        |    lc_mss_city,
        |    lc_mss_area
        |from
        |(
        |select row_number() over(partition by bu, id order by call_date desc) as uuid,*
        |from ods_sftm_new.ods_cust_store_tmp
        |) as t1
        |where t1.uuid = 1
      """.stripMargin)

    df_uni_store.write.mode(SaveMode.Overwrite).saveAsTable("ods_sftm_new.ods_cust_store")
//  下面的repartition, 产生N个HDFS文件, 也可以按照列来产生...具体可以参考API文档.
//    df_uni_store.repartition(1).write.mode(SaveMode.Overwrite).saveAsTable("ods_sftm_new.ods_cust_store")
    spark.stop()
  }

}
