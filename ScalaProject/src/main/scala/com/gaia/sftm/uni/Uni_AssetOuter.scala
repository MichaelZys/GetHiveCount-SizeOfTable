package com.gaia.sftm.uni

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

/**
  * @author michael
  * @create 2020-03-03 10:03
  */
object Uni_AssetOuter {
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
      .appName("Uni_AssetOuter")
//            .master("local[*]")
      .enableHiveSupport()
      .getOrCreate()

    import spark.implicits._

    val df_uni_assetOuter: DataFrame = spark.sql(
      """
        |select
        |    deposit_value,
        |    business_license,
        |    cost_value,
        |    customer_manager,
        |    case id when '' then ''
        |        else concat(bu,'_',id) end as gaid,
        |    dept_id,
        |    product_class,
        |    asset_code,
        |    throwin_cus_code,
        |    throwin_cus_id,
        |    case throwin_cus_id_str when '' then ''
        |        else concat(bu,'_',throwin_cus_id_str) end as cust_gaid,
        |    is_mislaid,
        |    product_vendor,
        |    throwin_contact,
        |    deposit_type,
        |    throwin_tel,
        |    product_type,
        |    throwin_date,
        |    net_value,
        |    product_code,
        |    product_brand,
        |    is_throwin,
        |    check_date,
        |    throwin_idcard,
        |    bu,
        |    call_date,
        |    exts
        |from
        |(
        |    select row_number() over(partition by bu,id order by call_date desc) as uuid, * from ods_sftm_new.ods_assetouter_tmp
        |) as t1
        |where t1.uuid = 1
      """.stripMargin)

    df_uni_assetOuter.write.mode(SaveMode.Overwrite).saveAsTable("ods_sftm_new.ods_assetouter")
    //  下面的repartition, 产生N个HDFS文件, 也可以按照列来产生...具体可以参考API文档.
    //    df_uni_store.repartition(1).write.mode(SaveMode.Overwrite).saveAsTable("ods_sftm_new.ods_cust_store")
    spark.stop()
  }
}
