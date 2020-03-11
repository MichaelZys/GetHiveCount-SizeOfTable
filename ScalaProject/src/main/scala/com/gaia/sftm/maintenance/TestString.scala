package com.gaia.sftm.maintenance

/**
  * @author michael
  * @create 2020-03-06 11:42
  */
object TestString {

  def main(args: Array[String]): Unit = {


    var abc:String = "/warehouse/tablespace/managed/hive/ods_sftm_new.db/ods_cust_store/part-00036-e3ded1a7-d216-4957-8202-4f548b6be26b-c000.snappy.parquet"
    var arr:Array[String] = abc.split("/")
    var len:Int = arr.length
    println(len)
    var aaa:String = "/"
    for(x <- 1 to len-2) {
      aaa += arr(x)
      if (x < len-2)
        aaa += "/"
//      println(arr(x))
    }
    println(aaa)
  }

}
