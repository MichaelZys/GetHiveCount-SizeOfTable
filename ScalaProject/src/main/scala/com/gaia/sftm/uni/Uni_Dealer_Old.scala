package com.gaia.sftm.uni

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

/**
  * @author michael
  * @create 2020-03-02 17:21
  */
object Uni_Dealer_Old {
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
      .appName("Uni_Dealer")
//      .master("local[*]")
      .enableHiveSupport()
      .getOrCreate()

    import spark.implicits._

    val df_uni_dealer: DataFrame = spark.sql(
      """
        |select
        |    dealer_id,
        |    dealer_name,
        |    dealer_code,
        |    dealer_manager,
        |    dealer_manager_waiqin365_id,
        |    dealer_type,
        |    dealer_type_id,
        |    dealer_dept_id,
        |    dealer_dept_name,
        |    dealer_dept_waiqin365_id,
        |    dealer_district,
        |    uper_dealer,
        |    uper_dealer_id,
        |    waiqin365_uper_dealer_id,
        |    dealer_mss_province,
        |    dealer_mss_city,
        |    dealer_mss_area,
        |    dealer_mss_street,
        |    dealer_addr,
        |    dealer_delivery_addr,
        |    dealer_cooperate_status,
        |    dealer_source,
        |    dealer_trade,
        |    dealer_scale,
        |    dealer_tel,
        |    dealer_fax,
        |    dealer_post,
        |    dealer_remarks,
        |    tradingarea_big,
        |    tradingarea,
        |    tradingarea_level_code,
        |    tradingarea_level_name,
        |    dealer_district_id,
        |    dealer_district_create_time,
        |    dealer_district_modify_time,
        |    dealer_district_creator_name,
        |    dealer_district_modifyier_name,
        |    dealer_district_status,
        |    id,
        |    dealer_cooperate_status_id,
        |    dealer_level_id,
        |    dealer_approval_status,
        |    dealer_status,
        |    create_time,
        |    call_date,
        |    bu,
        |    dealer_level,
        |    linkmans,
        |    deliverys,
        |    exts,
        |    stores,
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
        |    select row_number() over(partition by bu,id order by call_date desc) as uuid, * from ods_sftm_new.ods_cust_dealer_tmp
        |) as t1
        |where t1.uuid = 1
      """.stripMargin)

    df_uni_dealer.write.mode(SaveMode.Overwrite).saveAsTable("ods_sftm_new.ods_cust_dealer")
    //  下面的repartition, 产生N个HDFS文件, 也可以按照列来产生...具体可以参考API文档.
    //    df_uni_store.repartition(1).write.mode(SaveMode.Overwrite).saveAsTable("ods_sftm_new.ods_cust_store")
    spark.stop()
  }
}
