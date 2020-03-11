package com.gaia.sftm.common

//psqlJDBC
//Publisher : PostgreSQL Global Development Group
//驱动地址：http://jdbc.postgresql.org/download.html => http://jdbc.postgresql.org/download/postgresql-9.3-1102.jdbc41.jar
//本地下载：http://files.cnblogs.com/piaolingzxh/postgresql-9.3-1102.jdbc41.jar.zip
import java.sql.{Connection, DriverManager, ResultSet}

/**
  * @author michael
  * @create 2020-03-04 16:42
  */
object JDBC_PG {
//  var conn_str = "jdbc:postgresql://192.168.0.104:5432/datamate"
//  var conn = DriverManager.getConnection(conn_str, "postgres", "postgres")
//  classOf[org.postgresql.Driver]

  def exec(sql:String): Unit ={

    var conn_str = "jdbc:postgresql://192.168.0.104:5432/datamate"
    var conn = DriverManager.getConnection(conn_str, "postgres", "postgres")
    classOf[org.postgresql.Driver]

    try {
      // Configure to be Read Only
      val statement = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)

      // Execute Query
//      val rs = statement.executeQuery("SELECT * FROM dataman.dwd_cust limit 1")
      val rs = statement.executeQuery(sql)
      var columnCount = rs.getMetaData().getColumnCount();
      // Iterate Over ResultSet
      while (rs.next) {
        for (i <- 1 to columnCount) {
          System.out.print(rs.getString(i) + "\t");
        }
        System.out.println();
      }
    } finally {
      conn.close
    }
  }

  def execute(sql:String): Boolean ={

    var conn_str = "jdbc:postgresql://192.168.0.104:5432/datamate"
    var conn = DriverManager.getConnection(conn_str, "postgres", "postgres")
    classOf[org.postgresql.Driver]

    try {
      var statement = conn.createStatement()

      // Execute 插入
      var rs = statement.execute(sql)
      println(rs)

      return rs

    } finally {
      conn.close
    }
  }

}
