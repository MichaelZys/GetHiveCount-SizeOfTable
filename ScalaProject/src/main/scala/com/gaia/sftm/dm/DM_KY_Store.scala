package com.gaia.sftm.dm

import org.apache.spark.SparkConf
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions._

/**
  * @author michael
  * @create 2020-02-18 22:31
  */
object DM_KY_Store {

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
      .appName("DM_KY_Store")
//      .master("local[*]")
      .enableHiveSupport()
      .getOrCreate()

    import spark.implicits._
    //    spark.sql("show databases").show()


    var df_store:DataFrame = spark.sql(
      """
        |select *,concat(cust_bu,'_',cust_wq_id) as cust_gaid from dwd_sftm.dwd_base_info
        |where cust_bu = 'ky'
      """.stripMargin)

    var df_assetouter:DataFrame = spark.sql(
      """
        |select cust_gaid,count(cust_gaid) as qty_frig, 1 as is_frig from ods_sftm_new.ods_assetouter
        |where bu = 'ky' and is_throwin = '1' and product_class = '冰箱'
        |group by cust_gaid
        |having count(cust_gaid)>=1
      """.stripMargin)

//    df_store.join(df_assetouter, df_store("cust_gaid") === df_assetouter("cust_gaid"), "left").show(5)
    var df:DataFrame = df_store.join(df_assetouter, Seq("cust_gaid"), "left")
//    df.write.mode(SaveMode.Overwrite).saveAsTable("dm_sftm.dm_ky_store")
//    下面这种写法为什么不对?
//    df_store.join(df_assetouter, $"cust_gaid" === $"cust_gaid", "left")

//    df.select("qty_frig").distinct().show()
//    df.select("is_frig").distinct().show()
    df.na.fill(Map("is_frig" -> 0, "qty_frig" -> 0))
      .withColumn("qty_frig", $"qty_frig".cast(DataTypes.IntegerType))
      .withColumn("is_frig", $"is_frig".cast(DataTypes.IntegerType))
      .write.mode(SaveMode.Overwrite).saveAsTable("dm_sftm.dm_ky_store")


//    df.withColumn("qty_frig", $"qty_frig".cast(DataTypes.IntegerType))
//    df.withColumn("is_frig", $"is_frig".cast(DataTypes.IntegerType))
//    df.printSchema()

//    dm_store.write.mode(SaveMode.Overwrite).saveAsTable("dm_sftm.dm_ky_store")

    spark.stop()
  }

//  def f(): Unit ={
//
//  }
}
