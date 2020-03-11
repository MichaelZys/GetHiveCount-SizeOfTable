package com.gaia

/**
  * @author michael
  * @create 2020-02-18 22:25
  */
object test {
  def main(args: Array[String]): Unit = {
    println("First Scala Project")
    val a = new Animal()
    a.action()
    a.sleep("Nika")
  }
}

class Animal{
  def action(): Unit ={
    println("东吴可以喝水")
  }

  def sleep(name:String): Unit ={
    println(s"${name} 爱睡懒觉")
  }
}