package com.gaia.sftm.common

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

/**
  * @author michael
  * @create 2020-02-27 16:44
  */
object Get_Visit_Wide {

  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "hadoop");
    System.setProperty("user.name", "hadoop");

    val spark = SparkSession
      .builder()
      .appName("Get_Visit_Wide")
//      .master("local[*]")
      .enableHiveSupport()
      .getOrCreate()


    spark.table("ods_sftm_new.ods_visit").createOrReplaceTempView("v1")

    spark.table("dwd_sftm.dwd_base_info").createOrReplaceTempView("b1")


    val tt = spark.sql(
      """
        |SELECT v1.*,b1.*, unix_timestamp() as data_version
        |FROM v1
        |left join b1 on v1.customer=b1.cust_wq_id and v1.bu = b1.cust_bu
      """.stripMargin)

    tt.write.mode(SaveMode.Overwrite).saveAsTable("dwd_sftm.dwd_visit_wide")
  }

}
