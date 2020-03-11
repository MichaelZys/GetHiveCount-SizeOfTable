package com.gaia.sftm.uni

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

/**
  * @author michael
  * @create 2020-03-02 18:04
  */
object Uni_Product {
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
      .appName("Uni_Product")
//      .master("local[*]")
      .enableHiveSupport()
      .getOrCreate()

    import spark.implicits._

    val df_uni_product: DataFrame = spark.sql(
      """
        |select
        |    prd_id,
        |    status,
        |    prd_waiqin365_id,
        |    prd_code,
        |    prd_short_code,
        |    prd_barcode,
        |    prd_name,
        |    class_name,
        |    prd_spec,
        |    prd_brand,
        |    prd_unit,
        |    prd_valid_period,
        |    prd_weight,
        |    prd_sequ,
        |    prd_remarks,
        |    create_time,
        |    with_tag_new,
        |    with_tag_hot,
        |    with_tag_gift,
        |    with_tag_sale,
        |    with_tag_ex1,
        |    prd_suggest_price,
        |    prd_cost_price,
        |    prd_price,
        |    prd_sale_status,
        |    prd_short_name,
        |    prd_teu_coefficient,
        |    prd_same_price_code,
        |    creator_id,
        |    create_name,
        |    modifyier_id,
        |    modifyier_name,
        |    modifier_time,
        |    call_date,
        |    bu,
        |    prd_exts,
        |    prd_units
        |from
        |(
        |    select row_number() over(partition by prd_waiqin365_id order by call_date desc) as uuid, * from ods_sftm_new.ods_product_tmp
        |) as t1
        |where t1.uuid = 1
      """.stripMargin)

    df_uni_product.write.mode(SaveMode.Overwrite).saveAsTable("ods_sftm_new.ods_product")
    //  下面的repartition, 产生N个HDFS文件, 也可以按照列来产生...具体可以参考API文档.
    //    df_uni_store.repartition(1).write.mode(SaveMode.Overwrite).saveAsTable("ods_sftm_new.ods_cust_store")
    spark.stop()
  }
}
