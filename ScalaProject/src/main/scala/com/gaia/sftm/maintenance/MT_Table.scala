package com.gaia.sftm.maintenance

import org.apache.spark.sql.{SparkSession, DataFrame}
//import scala.collection.mutable.ListBuffer

import com.gaia.sftm.common.Lib
import com.gaia.sftm.common.MDataTime
import com.gaia.sftm.common.JDBC_PG


/**
  * @author michael
  * @create 2020-03-04 17:13
  */
object MT_Table {

  val list_table:List[String] = List(
    "ods_sftm_new.ods_assetouter",
    "ods_sftm_new.ods_assetouter_tmp",
    "ods_sftm_new.ods_cust_dealer",
    "ods_sftm_new.ods_cust_dealer_tmp",
    "ods_sftm_new.ods_cust_store",
    "ods_sftm_new.ods_cust_store_tmp",
    "ods_sftm_new.ods_cust_type",
    "ods_sftm_new.ods_cust_type_tmp",
    "ods_sftm_new.ods_order_dt",
    "ods_sftm_new.ods_order_dt_tmp",
    "ods_sftm_new.ods_order_zy",
    "ods_sftm_new.ods_order_zy_tmp",
    "ods_sftm_new.ods_org_dep",
    "ods_sftm_new.ods_org_dep_tmp",
    "ods_sftm_new.ods_org_emp",
    "ods_sftm_new.ods_org_emp_tmp",
    "ods_sftm_new.ods_product",
    "ods_sftm_new.ods_product_tmp",
    "ods_sftm_new.ods_visit",
    "ods_sftm_new.ods_visit_tmp",
    "dwd_sftm.dwd_base_info",
    "dwd_sftm.dwd_cust",
    "dwd_sftm.dwd_order_dt",
    "dwd_sftm.dwd_visit_wide",
    "dm_sftm.dm_km_store",
    "dm_sftm.dm_ky_store")

  def main(args: Array[String]): Unit = {

//    var a = 0;
//    val numList = List(1,2,3,4,5,6);
//
//    // for 循环
//    for( a <- numList ){
//      println( "Value of a: " + a );
//    }

    val spark = SparkSession
      .builder()
      .appName("Maintenance Count of Table")
      .master("local[*]")
      .enableHiveSupport()
      .getOrCreate()

//    val jdbc = JDBC_PG

    for(x <- list_table) {
      println(x)
      //    scala 字符串拼接 var a = "had" var b = "oop"
      //    println(s"$a+$b") //获取参数的值通过$符号获取,并且在获取前加上一个s
      val df: DataFrame = spark.sql(s"select count(*) from $x")

      //      println(df.collect(){0}{0}.toString())

      var cot: String = df.collect() {0} {0}.toString()

      var sot_b:Long = getTableHDFSTotalSize(x)

      var sot_kb:Double = 0
      var sot_mb:Double = 0
      var sot_gb:Double = 0

      if(sot_b != 0) {
        sot_kb = sot_b / 1024
        sot_mb = sot_kb / 1024
        sot_gb = sot_mb / 1024
      }

      println(s"表$x 有$cot 行")
      println(s"表$x Size为" + sot_b.toString)
      var sql:String = "insert into tab_detail(db_name, tab_name, cot, sot_b, sot_kb, sot_mb, sot_gb, data_version)" +
        " values('%s','%s','%s','%s','%s','%s','%s','%s');".format(x.split("\\.")(0),x.split("\\.")(1),cot,sot_b,sot_kb,sot_mb,sot_gb,MDataTime.NowDate())

      println(sql)

      JDBC_PG.execute(sql)

    }

  }





  def getTableHDFSTotalSize(tableName:String): Long ={
//    println(tableName)
    // 拼接tableName, 用于获得对应的HDFS Direct.
    var arr:Array[String] = tableName.split("\\.")
//    for(x<-arr)
//      println(x)
    var tableDirect:String = "hdfs://192.168.0.103:8020/warehouse/tablespace/managed/hive/" + arr(0) + ".db/" + arr(1)
    println("拼接出来的HDFS地址")
    println(tableDirect)

    //这里有一个方法, 传入HDFS地址, 返回大小(以b为单位)
    var totalSize:Long = Lib.getHDFSDirectTotalSize(tableDirect)

    return totalSize
  }

}