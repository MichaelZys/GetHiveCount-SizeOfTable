package com.gaia.sftm.common

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

/**
  * @author michael
  * @create 2020-03-08 21:01
  */
object MDataTime {

  def NowDate(): String = {
    val now: Date = new Date()
    val dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val date = dateFormat.format(now)
    return date
  }

}
