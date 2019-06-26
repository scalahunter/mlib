package cn.spark.study.mlib.akg

import org.apache.spark.internal.Logging

trait AKGObject extends Serializable with Logging {

  private var sample: Boolean = false //是否是样本
  private var center: Boolean = false //是否是质点中心
  private var level: Int = -1 //分类 样本值

  def setSample(_sample: => Boolean) {
    this.sample = _sample
  }
  def isSample(): Boolean = {
       sample
  }

  def setCenter(_center: => Boolean) {
    this.center = _center
  }
  def isCenter(): Boolean = {
    center
  }

  def setLevel(_level: => Int) {
    this.level = _level
  }

  def getLevel(): Int = {
    level
  }

}