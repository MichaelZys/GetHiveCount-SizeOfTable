package com.gaia.sftm.common

import java.net.URI

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, FileUtil, Path}

import scala.collection.mutable.ArrayBuffer

/**
  * @author michael
  * @create 2020-03-06 17:07
  * @define 用来写一些功能类
  */
object Lib {

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

  //获取字符串指定部分
  /**
    * @param str 字符串
    * @return 指定部分
    */
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
  /**
    * 获取指定目录下文件, 返回文件的名称和大小
    * @param path 指定目录
    * @return Map(文件名,大小)
    */
  def getAllFiles_map(path: String): scala.collection.mutable.Map[String,Long] = {
//    val arr = ArrayBuffer[String]()
    val mmap = scala.collection.mutable.Map[String,Long]()
    val hdfs = getHdfs(path)
    val getPath = getFilesAndDirs(path)
    getPath.foreach(patha => {
      if (hdfs.getFileStatus(patha).isFile()){
//        arr += patha.toString
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


  def getAllFiles(path: String): scala.collection.mutable.Map[String,Long] = {
    //    val arr = ArrayBuffer[String]()
    val mmap = scala.collection.mutable.Map[String,Long]()
    val hdfs = getHdfs(path)
    val getPath = getFilesAndDirs(path)
    getPath.foreach(patha => {
      if (hdfs.getFileStatus(patha).isFile()){
        //        arr += patha.toString
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
  /**
    * 获取该目录, 和大小
    * @param path
    * @return
    */
  def getSameHDFSDirectTotalSize(path:String): scala.collection.mutable.Map[String,Long] ={
    var map = scala.collection.mutable.Map[String,Long]()
    for((key,value)<- getAllFiles_map(path)) {
      //首先看key在map中是否存在, 因为key是截断过的.
      if(map.contains(getSpecialChar(key))){
        //map中存在相同的key, 就在原有的value基础上+新的value
        map(getSpecialChar(key)) = map(getSpecialChar(key)) + value
      }else{
        //map中不存在相同的key, 就新建一个
        map += (getSpecialChar(key) -> value)
      }
    }
    return map
  }

  /**
    * 获得指定目录下, 所有文件的大小
    * @param directHDFS 指定目录
    * @return Size(b)
    */
  def getHDFSDirectTotalSize(directHDFS:String): Long ={

    var totalSize:Long = 0

    for( (key, value) <- getSameHDFSDirectTotalSize(directHDFS)) {
      println("文件" + key +", 大小" + value.toString + "b")
      totalSize = value
    }

    return totalSize

  }

}
