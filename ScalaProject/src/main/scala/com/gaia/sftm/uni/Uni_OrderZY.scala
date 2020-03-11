package com.gaia.sftm.uni

import org.apache.spark.SparkConf
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{ArrayType, StringType, StructType}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

/**
  * @author michael
  * @create 2020-03-03 10:24
  */
object Uni_OrderZY {
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
      .appName("Uni_OrderZY")
//                  .master("local[*]")
      .enableHiveSupport()
      .getOrCreate()

    import spark.implicits._

    //先对直营订单去重
    val sql:String =
      """
        |select * from (
        |     select row_number() over(partition by bu, id order by call_date desc) as uuid, *
        |     from  ods_sftm_new.ods_order_zy_tmp) as t1
        |where t1.uuid = 1
        |""".stripMargin

    val df: DataFrame = spark.sql(sql)

    //prods字段的schema
    val prodJsonSchema = new StructType()
      .add("detail_id",StringType)
      .add("product_id",StringType)
      .add("product_name",StringType)
      .add("product_code",StringType)
      .add("product_source_code",StringType)
      .add("purchase_price",StringType)
      .add("purchase_count",StringType)
      .add("purchase_input_unit",StringType)
      .add("purchase_input_unit_name",StringType)
      .add("base_unit",StringType)
      .add("base_unit_name",StringType)
      .add("purchase_base_unit_count",StringType)
      .add("purchase_amount",StringType)
      .add("purchase_discount_amount",StringType)
      .add("remark",StringType)
      .add("is_gift",StringType)
      .add("promotion_code",StringType)
      .add("promotion_name",StringType)
      .add("promotion_type",StringType)
      .add("promotion_tag",StringType)
      .add("promotion_calc_mode",StringType)
      .add("promotion_calc_type",StringType)
      .add("superposition_promotion_code",StringType)
      .add("superposition_promotion_name",StringType)
      .add("amount_promotion_code",StringType)
      .add("amount_promotion_name",StringType)
      .add("exts",StringType)
      .add("posnr",StringType)

    val itemsDF: DataFrame = df.select($"*", explode_outer(from_json($"prods", ArrayType(prodJsonSchema))).as("items"))

    //将prods字段解析后的结构
    val df_uni_orderZY: DataFrame = itemsDF.select($"id", $"status", $"purchase_no", $"date", $"emp_dept_id", $"emp_dept_source_code",
      $"emp_id", $"emp_code", $"emp_name", $"emp_source_code", $"trade_type", $"cm_id", $"cm_code", $"cm_name", $"cm_source_code",
      $"cm_dept_id", $"cm_dept_name", $"cm_dept_code", $"cm_dept_source_code", $"purchase_status", $"purchase_business_status",
      $"consignment_date", $"order_amount", $"order_discount_amount", $"pictures", $"creator_id", $"create_name", $"create_time_str",
      $"modify_time_str", $"confirm_emp_id", $"confirm_emp_name", $"confirm_time_str", $"confirm_reason", $"reconfirm_emp_id",
      $"reconfirm_emp_name", $"reconfirm_time_str", $"reconfirm_reason", $"receive_name", $"receive_phone", $"receive_tel",
      $"receive_addr", $"remark", $"visit_implement_id", $"order_source", $"call_date", $"bu",$"exts",
      substring($"date", 1, 7).alias("month"),
      $"items.detail_id".alias("prod_detail_id"), $"items.product_id".alias("prod_product_id"),$"items.product_name".alias("prod_product_name"),
      $"items.product_code".alias("prod_product_code"),$"items.product_source_code".alias("prod_product_source_code"),$"items.purchase_price".alias("prod_purchase_price"),
      $"items.purchase_count".alias("prod_purchase_count"),$"items.purchase_input_unit".alias("prod_purchase_input_unit"),
      $"items.purchase_input_unit_name".alias("prod_purchase_input_unit_name"),$"items.base_unit".alias("prod_base_unit"),
      $"items.base_unit_name".alias("prod_base_unit_name"),$"items.purchase_base_unit_count".alias("prod_purchase_base_unit_count"),
      $"items.purchase_amount".alias("prod_purchase_amount"),$"items.purchase_discount_amount".alias("prod_purchase_discount_amount"),
      $"items.remark".alias("prod_remark"),$"items.is_gift".alias("prod_is_gift"),$"items.promotion_code".alias("prod_promotion_code"),
      $"items.promotion_name".alias("prod_promotion_name"),$"items.promotion_type".alias("prod_promotion_type"),
      $"items.promotion_tag".alias("prod_promotion_tag"),$"items.promotion_calc_mode".alias("prod_promotion_calc_mode"),
      $"items.promotion_calc_type".alias("prod_promotion_calc_type"),$"items.superposition_promotion_code".alias("prod_superposition_promotion_code"),
      $"items.superposition_promotion_name".alias("prod_superposition_promotion_name"), $"items.amount_promotion_code".alias("prod_amount_promotion_code"),
      $"items.amount_promotion_name".alias("prod_amount_promotion_name"), $"items.exts".alias("prod_exts"),$"items.posnr".alias("prod_posnr")
    )

    df_uni_orderZY.write.mode(SaveMode.Overwrite).partitionBy("bu", "month").saveAsTable("dwd_sftm.dwd_order_zy")
    //  下面的repartition, 产生N个HDFS文件, 也可以按照列来产生...具体可以参考API文档.
    //    df_uni_store.repartition(1).write.mode(SaveMode.Overwrite).saveAsTable("ods_sftm_new.ods_cust_store")
    spark.stop()
  }
}
