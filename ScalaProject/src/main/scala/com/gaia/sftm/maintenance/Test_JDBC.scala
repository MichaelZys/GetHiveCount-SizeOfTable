package com.gaia.sftm.maintenance

import com.gaia.sftm.common.{JDBC_PG, MDataTime}

/**
  * @author michael
  * @create 2020-03-05 10:54
  */
object Test_JDBC {

  def main(args: Array[String]): Unit = {

//    var sql:String = "insert into tab_detail(db_name, tab_name, cot, sot_b, sot_kb, sot_mb, sot_gb, data_version)" +
//      " values('ods_sftm_new', 'ods_assetouter', 2, 2, 2, 2, 2, '2020-03-08 20:46:00');"

    var sql1:String = "insert into tab_detail(db_name, tab_name, cot, sot_b, sot_kb, sot_mb, sot_gb, data_version)" +
      " values('%s','%s','%d','%d','%d','%d','%d','%s');".format(1,2,3,4,5,6,7,MDataTime.NowDate())

    print(sql1)

    print(JDBC_PG.execute(sql1))
    print(JDBC_PG.execute(sql1))
  }

}
