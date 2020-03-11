package com.gaia.sftm.uni

import org.apache.spark.sql.functions._
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object Uni_Dealer {
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
      .appName("Uni_Dealer_New")
//            .master("local[*]")
      .enableHiveSupport()
      .getOrCreate()

    import spark.implicits._

    val df: DataFrame = spark.sql(
      """
        |select *
        |from
        |(
        |    select row_number() over(partition by bu,id order by call_date desc) as uuid, * from ods_sftm_new.ods_cust_dealer_tmp
        |) as t1
        |where t1.uuid = 1
      """.stripMargin)

    val df_uni_dealer:DataFrame = df.select($"dealer_id",$"dealer_name",$"dealer_code",$"dealer_manager"
      ,$"dealer_manager_waiqin365_id", $"dealer_type",$"dealer_type_id",$"dealer_dept_id",$"dealer_dept_name"
      ,$"dealer_dept_waiqin365_id",$"dealer_district",$"uper_dealer", $"uper_dealer_id", $"waiqin365_uper_dealer_id"
      ,$"dealer_mss_province",$"dealer_mss_city",$"dealer_mss_area",$"dealer_mss_street", $"dealer_addr"
      ,$"dealer_delivery_addr",$"dealer_cooperate_status", $"dealer_source",$"dealer_trade",$"dealer_scale"
      ,$"dealer_tel",$"dealer_fax",$"dealer_post",$"dealer_remarks",$"tradingarea_big", $"tradingarea"
      ,$"tradingarea_level_code",$"tradingarea_level_name",$"dealer_district_id",$"dealer_district_create_time"
      ,$"dealer_district_modify_time",$"dealer_district_creator_name",$"dealer_district_modifyier_name"
      ,$"dealer_district_status",$"id", $"dealer_cooperate_status_id",$"dealer_level_id",$"dealer_approval_status"
      ,$"dealer_status",$"create_time",$"call_date",$"bu",$"dealer_level", $"linkmans",$"deliverys",$"exts",$"stores"
      ,$"lc_id",$"lc_cm_id",$"lc_cm_code",$"lc_cm_name",$"lc_location_a",$"lc_location_c",$"lc_mss_province"
      ,$"lc_mss_city",$"lc_mss_area")

    df_uni_dealer.write.mode(SaveMode.Overwrite).saveAsTable("ods_sftm_new.ods_cust_dealer")

    val df_uni_dealer_new:DataFrame = df.select(
      $"dealer_id".alias("o_dealer_id"),
      $"dealer_name".alias("o_dealer_name"),
      $"dealer_code".alias("o_dealer_code"),
      $"dealer_manager".alias("o_dealer_manager"),
      $"dealer_manager_waiqin365_id".alias("o_dealer_manager_waiqin365_id"),
      $"dealer_type".alias("o_dealer_type"),
      $"dealer_type_id".alias("o_dealer_type_id"),
      $"dealer_dept_id".alias("o_dealer_dept_id"),
      $"dealer_dept_name".alias("o_dealer_dept_name"),
      $"dealer_dept_waiqin365_id".alias("o_dealer_dept_waiqin365_id"),
      $"dealer_district".alias("o_dealer_district"),
      $"uper_dealer".alias("o_uper_dealer"),
      $"uper_dealer_id".alias("o_uper_dealer_id"),
      $"waiqin365_uper_dealer_id".alias("o_waiqin365_uper_dealer_id"),
      $"dealer_mss_province".alias("o_dealer_mss_province"),
      $"dealer_mss_city".alias("o_dealer_mss_city"),
      $"dealer_mss_area".alias("o_dealer_mss_area"),
      $"dealer_mss_street".alias("o_dealer_mss_street"),
      $"dealer_addr".alias("o_dealer_addr"),
      $"dealer_delivery_addr".alias("o_dealer_delivery_addr"),
      $"dealer_cooperate_status".alias("o_dealer_cooperate_status"),
      $"dealer_source".alias("o_dealer_source"),
      $"dealer_trade".alias("o_dealer_trade"),
      $"dealer_scale".alias("o_dealer_scale"),
      $"dealer_tel".alias("o_dealer_tel"),
      $"dealer_fax".alias("o_dealer_fax"),
      $"dealer_post".alias("o_dealer_post"),
      $"dealer_remarks".alias("o_dealer_remarks"),
      $"tradingarea_big".alias("o_tradingarea_big"),
      $"tradingarea".alias("o_tradingarea"),
      $"tradingarea_level_code".alias("o_tradingarea_level_code"),
      $"tradingarea_level_name".alias("o_tradingarea_level_name"),
      $"dealer_district_id".alias("o_dealer_district_id"),
      $"dealer_district_create_time".alias("o_dealer_district_create_time"),
      $"dealer_district_modify_time".alias("o_dealer_district_modify_time"),
      $"dealer_district_creator_name".alias("o_dealer_district_creator_name"),
      $"dealer_district_modifyier_name".alias("o_dealer_district_modifyier_name"),
      $"dealer_district_status".alias("o_dealer_district_status"),
      $"id".alias("o_id"),
      $"dealer_cooperate_status_id".alias("o_dealer_cooperate_status_id"),
      $"dealer_level_id".alias("o_dealer_level_id"),
      $"dealer_approval_status".alias("o_dealer_approval_status"),
      $"dealer_status".alias("o_dealer_status"),
      $"create_time".alias("o_create_time"),
      $"dealer_level".alias("o_dealer_level"),
      $"linkmans".alias("o_linkmans"),
      $"deliverys".alias("o_deliverys"),
      $"exts".alias("o_exts"),
      $"stores".alias("o_stores"),
      $"lc_id".alias("o_lc_id"),
      $"lc_cm_id".alias("o_lc_cm_id"),
      $"lc_cm_code".alias("o_lc_cm_code"),
      $"lc_cm_name".alias("o_lc_cm_name"),
      $"lc_location_a".alias("o_lc_location_a"),
      $"lc_location_c".alias("o_lc_location_c"),
      $"lc_mss_province".alias("o_lc_mss_province"),
      $"lc_mss_city".alias("o_lc_mss_city"),
      $"lc_mss_area".alias("o_lc_mss_area"),
      $"call_date".alias("ga_call_date"),
      $"bu".alias("ga_bu"),
      concat_ws("_",$"bu", $"id").alias("ga_id")
    )

    df_uni_dealer_new.write.mode(SaveMode.Overwrite).saveAsTable("ods_sftm_new.ods_cust_dealer_new")
    //  下面的repartition, 产生N个HDFS文件, 也可以按照列来产生...具体可以参考API文档.
    //    df_uni_store.repartition(1).write.mode(SaveMode.Overwrite).saveAsTable("ods_sftm_new.ods_cust_store")
    spark.stop()
  }
}
