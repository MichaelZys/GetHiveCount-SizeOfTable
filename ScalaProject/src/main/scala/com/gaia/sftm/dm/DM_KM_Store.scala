package com.gaia.sftm.dm

import org.apache.spark.SparkConf
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions._

/**
  * @author michael
  * @create 2020-02-21 16:55
  */
object DM_KM_Store {

  def main(args: Array[String]): Unit = {


    System.setProperty("HADOOP_USER_NAME", "hadoop")

    val conf = new SparkConf
    conf.set("spark.sql.adaptive.enabled", "true")
    conf.set("spark.sql.adaptive.shuffle.targetPostShuffleInputSize", "128M")
    conf.set("spark.sql.adaptive.join.enabled", "true")
    conf.set("spark.sql.autoBroadcastJoinThreshold", "20971520")

    val spark = SparkSession
      .builder()
      .config(conf)
      .appName("DM_KM_Store")
//      .master("local[*]")
      .enableHiveSupport()
      .getOrCreate()

    import spark.implicits._

    var df_store:DataFrame = spark.sql(
      """
        |select *,concat(cust_bu,'_',cust_wq_id) as cust_gaid from dwd_sftm.dwd_base_info
        |where cust_bu = 'km'
      """.stripMargin)


//    df_store.select("cust_type").distinct().show()

//    取字符串最后一个值
//    var sss:String = "abc"
//    sss.takeRight(1)

    df_store
      .withColumn("qty_shelves", regexp_extract($"cust_type", "\\d+", 0))
//      .na.replace(["Alice", None],"qty_shelves")
//      .select("cust_type","qty_shelves").distinct().show()
      .write.mode(SaveMode.Overwrite).saveAsTable("dm_sftm.dm_km_store")
    spark.stop()



  }

}
