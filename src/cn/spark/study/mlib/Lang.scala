package cn.spark.study.mlib
import scala.math.pow

import cn.spark.study.mlib.akg.AKGObject

object Lang {

  case class Point(x: Double, y: Double)

  //长试形四个点
  case class Rect(a: Point, b: Point, c: Point, d: Point);

  case class AKGCase(point: Point) extends AKGObject {
    var id: Int = 0
  }
  
   /**
   *  计算两个的点距离的平方(欧氏距离)
   *  公式  x1 - x2 的平方  + y1 - y2的平方 ，的结果集再开根.
   */
  def distanceSquared(p1: Point, p2: Point): Double = {
    val distance: Double = pow((p1.x - p2.x).doubleValue(), 2) + pow((p1.y - p2.y).doubleValue(), 2)
    Math.sqrt(distance)
  }

  /**
   *  计算两个点的和
   */
  def addPoints(p1: Point, p2: Point): Point = {
    new Point(p1.x + p2.x, p1.y + p2.y)
  }

  

  //求圆心
  def getCircleRadisByRect(rect: Rect): Point = {
    getRadis(rect.a, rect.b, rect.c)
  }

  //求圆心
  def getRadis(p1: Point, p2: Point, p3: Point): Point = {
    var a = 2 * (p2.x - p1.x);
    var b = 2 * (p2.y - p1.y);
    var c = p2.x * p2.x + p2.y * p2.y - p1.x * p1.x - p1.y * p1.y;
    var d = 2 * (p3.x - p2.x);
    var e = 2 * (p3.y - p2.y);
    var f = p3.x * p3.x + p3.y * p3.y - p2.x * p2.x - p2.y * p2.y;
    var x = (b * f - e * c) / (b * d - e * a);
    var y = (d * c - a * f) / (b * d - e * a);
    var p = new Point(x, y);
    var r = Math.sqrt((p.x - p1.x) * (p.x - p1.x) + (p.y - p1.y) * (p.y - p1.y)); //半径
    p;
  }

  // 获取长方形的中心点
  def getRectRadis(rect: Rect): Point = {
    new Point((rect.b.x - rect.a.x) / 2 + rect.a.x, (rect.c.y - rect.a.y) / 2 + rect.a.y)
  }

  def rect2String(rect: Rect): String = {
    var a = "a(" + rect.a.x + "," + rect.a.y + ")";
    var b = "b(" + rect.b.x + "," + rect.b.y + ")";
    var c = "c(" + rect.c.x + "," + rect.c.y + ")";
    var d = "d(" + rect.d.x + "," + rect.d.y + ")";
    a + "," + b + "," + c + "," + d
  }

}