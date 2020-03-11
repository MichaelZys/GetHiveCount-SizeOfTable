package com.gaia.sftm.maintenance

import java.net.URI
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, FileUtil, Path}
import scala.collection.mutable.ArrayBuffer


/**
  * @author michael
  * @create 2020-03-05 15:35
  */
object TestFile {
  def main(args: Array[String]): Unit = {
    val path = "hdfs://192.168.0.103:8020/warehouse/tablespace/managed/hive/ods_sftm_new.db"
    //生成FileSystem
    def getHdfs(path: String): FileSystem = {
      val conf = new Configuration()
      FileSystem.newInstance(URI.create(path), conf)
    }

    //获取目录下的一级文件和目录
    def getFilesAndDirs(path: String): Array[Path] = {
      val fs = getHdfs(path).listStatus(new Path(path))
      FileUtil.stat2Paths(fs)
    }
    //获取目录下的一级文件
    def getFiles(path: String): Array[String] = {
      getFilesAndDirs(path).filter(getHdfs(path).getFileStatus(_).isFile())
        .map(_.toString)
    }
    //获取目录下的一级目录
    def getDirs(path: String): Array[String] = {
      getFilesAndDirs(path).filter(getHdfs(path).getFileStatus(_).isDirectory)
        .map(_.toString)
    }
    //获取目录下的所有文件
//    def getAllFiles(path: String): ArrayBuffer[String] = {
//      val arr = ArrayBuffer[String]()
//      val mmap = scala.collection.mutable.Map[String,String]()
//      val hdfs = getHdfs(path)
//      val getPath = getFilesAndDirs(path)
//      getPath.foreach(patha => {
//        if (hdfs.getFileStatus(patha).isFile()){
//          arr += patha.toString
////          hdfs.getFileStatus(patha)
//          mmap += ( patha.toString -> hdfs.getFileStatus(patha).getLen.toString)
////          println(patha)
////          println(hdfs.getFileStatus(patha).getLen)
//      }
//        else {
//          arr ++= getAllFiles(patha.toString())
//        }
//
//
//      })
//      arr
//    }

    def getSpecialChar(str:String): String ={

      // 用"/"切分字符串
      var arr:Array[String] = str.split("/")
      // 获取大小
      var len:Int = arr.length
//      println(len)
      var aaa:String = "/"

      for(x <- 1 to len-2) {
        aaa += arr(x)
        if (x < len-2)
          aaa += "/"
        //      println(arr(x))
      }

      return aaa

    }


    //获取目录下的所有文件
    def getAllFiles_map(path: String): scala.collection.mutable.Map[String,Long] = {
      val arr = ArrayBuffer[String]()
      val mmap = scala.collection.mutable.Map[String,Long]()
      val hdfs = getHdfs(path)
      val getPath = getFilesAndDirs(path)
      getPath.foreach(patha => {
        if (hdfs.getFileStatus(patha).isFile()){
          arr += patha.toString
          //          hdfs.getFileStatus(patha)
//          println(patha.toString)
          mmap += ( patha.toString.replace("hdfs://192.168.0.103:8020","") -> hdfs.getFileStatus(patha).getLen)
          //          println(patha)
          //          println(hdfs.getFileStatus(patha).getLen)
        }
        else {
          mmap ++= getAllFiles_map(patha.toString())
        }


      })
      mmap
    }

    //获取Map(hdfs文件,size),中相同路径下文件的总大小
    def getSameHDFSDirectTotalSize(): scala.collection.mutable.Map[String,Long] ={
      var map = scala.collection.mutable.Map[String,Long]()

      for((key,value)<- getAllFiles_map(path)) {



        //首先看key在map中是否存在, 因为key是截断过的.
        if(map.contains(getSpecialChar(key))){

//          map中存在相同的key, 就在原有的value基础上+新的value

          map(getSpecialChar(key)) = map(getSpecialChar(key)) + value

        }else{

//          map中不存在相同的key, 就新建一个
          map += (getSpecialChar(key) -> value)

        }


      }
      return map
    }

//    println("获取目录下的一级文件和目录")
//    getFilesAndDirs(path).foreach(println)
//    getHdfs(path).getFileStatus(_).getBlockSize

//    println("获取目录下的一级文件")
//    getFiles(path).foreach(println)
//    println("获取目录下的一级目录")
//    getDirs(path).foreach(println)
    println("获取目录下所有文件")
//    getAllFiles_map(path).foreach(println)



    for((key,value)<- getSameHDFSDirectTotalSize){

      println(key + ":" + value);

    }

//    println(getSpecialChar("/warehouse/tablespace/managed/hive/ods_sftm_new.db/ods_cust_store/aaa"))

    }
}
