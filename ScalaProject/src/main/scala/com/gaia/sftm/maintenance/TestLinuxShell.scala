package com.gaia.sftm.maintenance

import scala.sys.process._

/**
  * @author michael
  * @create 2020-03-05 14:15
  */
object TestLinuxShell {

  def main(args: Array[String]): Unit = {
//    val tt = "test@163.com"
//    val title = "title"
//    val mails = "test@163.com"
//    val content = "content"
    val send_mail_cmd = "hadoop fs -ls  /warehouse/tablespace/managed/hive/ods_sftm_new.db/ods_assetouter|awk -F ' ' '{print $5}'|awk '{a+=$1}END{print a}'"
    println(send_mail_cmd)
    s"$send_mail_cmd"!
  }

}
