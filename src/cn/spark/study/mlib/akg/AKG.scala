package cn.spark.study.mlib.akg

import scala.math.pow
import scala.Array
import org.apache.spark.SparkContext
import cn.spark.study.mlib.Lang
import cn.spark.study.mlib.Lang.{Point,AKGCase}
import scala.collection.mutable.ArraySeq
import java.util.ArrayList
/**
 * 
 * @author alex rainy_2004@126.com
 * 
 * */
case class AKG[A <: AKGCase](sc: SparkContext, arrays: Array[A]) {
 
  case class AKGType( sample:A, ids: Array[Int], sum: Double) {
     val idsLen = ids.length; //成员数据
  }

  /**
   * 给数据设置id
   * 
   * */
  def init(): Unit = {
    //这里都是main线程
    var j = 1 ; 
    arrays.foreach(f => {
       f.id =j;
       j = j+1; 
      // println("for each"+f.id+":"+Thread.currentThread().getName);
     });
  }
  
 
  /**
   * 计算一群点中距离某个点最近的点的角标
   */
  def sumPoints(onePoint: Point, points: Array[Point]): Double = {
    var bestIndex = 0
    var closest: Double = Double.MaxValue 
    var sum = 0.0D
    for (i <- points.indices) {
      sum += Lang.distanceSquared(onePoint, points(i))
    }
    sum
  }

  /**
   * 在一堆数据当中，找出中心点
   *
   */
  def findCenterPoint(points: Array[Point]): Point = {
    var center: Point = points(0);
    var lastsum: Double = -1.0D;
    //增强for循环
    points.foreach(e => {
      val sum = sumPoints(e, points)
     // println("(" + e.x + "," + e.y + ")=" + sum)
      if (sum <= lastsum) {
        center = e;
      }
      lastsum = sum;
    });
    center
  }

  /**
   * 在一堆数据当中，找出中心点
   */
  def findCenterCase(cases: Array[A]): A = {
    var center: A = cases(0);
    val points: Array[Point] = cases.map(f => f.point)
    var minSum: Double = -1.0D;
    cases.foreach(e => {
      val sum = sumPoints(e.point, points)
      //println(e.point.x+":"+sum)
      if (sum <= minSum) {
        center = e;
        minSum = sum
      } else if (minSum == -1.0D) {
        minSum = sum;
      }
    });
    center
  }

  // 计算一群点中距离某个点最近的点的角标
  def sumCases(oneCase: A, cases: Array[A]): Double = {
    var bestIndex = 0
    var closest: Double = Double.MaxValue
    var sum = 0.0D
    for (i <- cases.indices) {
      sum += Lang.distanceSquared(oneCase.point, cases(i).point)
    }
    sum
  }

  /**
   *  计算一群点中距离某个点最近的点的下标
   *  如果当前点是样本点，返回-1
   *  否则返回距离样本最近的角下标
   *
   */
  def closesCase(p: A, samples: Array[A]): Int = {
    if (p.isSample())  p.getLevel()
    var bestIndex = 0
    var closest: Double = Double.MaxValue
    for (i <- (0 until samples.length)) {
      val dist = Lang.distanceSquared(p.point, samples(i).point)
      if (dist < closest) {
        closest = dist
        bestIndex = i
      }
    }
    bestIndex
  }

  def takeSamples(n: Int): Array[A] = {
     val samples = arrays.take(n);
     for (i <- (0 until samples.length)) {
      samples(i).setSample(true)
      samples(i).setLevel(i)
    }
    samples
  }

  // 假如:二个样本点，然后所有元素向样本点归类。归类后,在原来二个样本点再找出新的两个样本点。
  // 然后所有元素又向新的两个样本点归类。又找出新的样本点。一直循环下去，直到找到最优值
  // 最优值就是
  // 每一堆的K值最小。(欧氏距离 / 样本数)
  def group(samples: Array[A]): (Array[AKGType], Double) = {
    for (array0 <- arrays) {
      val sampleKeyIndex = closesCase(array0, samples);
      array0.setLevel(sampleKeyIndex);
      // println("array0="+array0.point.x+",level:"+sampleKeyIndex)
    }
    val groups: Map[Int, Array[A]] = arrays.groupBy(_.getLevel())
    //获取每一组新的质点和计算
    val akgArrays = new Array[AKGType](samples.length)
    var sums: Double = 0D;
    for ((k, v) <- groups) {
      val center = samples(k);
      val sum = this.sumCases(center, v);
      val ids: Array[Int] = v.map(f => f.id);
      val akg = new AKGType(center, ids, sum);
      akgArrays(k) = akg;
      sums += (akg.sum / akg.idsLen).doubleValue();
    }
    (akgArrays, sums);
  }

  def getAById(id: Int): A = {
    var tag = true;
    var tem: A = arrays(0);
    for (arr0 <- arrays; if (tag)) {
      if (arr0.id == id) {
        tem = arr0;
        tag = false;
      }
    }
    tem;
  }

  def findCaseCenter0(akg: (Array[AKGType], Double),sampleNumber:Int): Array[A] = {
    val akgs: Array[AKGType] = akg._1;
    val sum = akg._2;
    var j = 0
   val copySamples = arrays.take(sampleNumber);
   copySamples.foreach(f => f.setSample(false));
    for (akg <- akgs) {
      val ids = akg.ids;
      val akgCases = arrays.take(ids.length)
      var i = 0;
      for (id <- ids) {
        akgCases(i) = getAById(id)
        i += 1
      }
      val center = this.findCenterCase(akgCases);
      copySamples(j) = center
      j += 1;
    }
    copySamples
  }

  def arrayT[A: Manifest](ary: A*): Array[A] = { //接受多个参数
    val arys = new Array[A](ary.length) //初始化一个数组
    for (item <- 0 until ary.length)
      arys(item) = ary(item)
    arys
  }

  def predict(samples: Array[A], predictTime: Int): (Array[AKGType], Double) = {
    var lastakg: (Array[AKGType], Double) = null;
    var tag = true;
    var equalTime = 0;
    var greaterTime = 0;
    for (i <- (0 until predictTime); if (tag)) {
      val akg: (Array[AKGType], Double) = group(samples);
      // 打印 akg
      val akgTypes: Array[AKGType] = akg._1
      val newscenters = findCaseCenter0(akg,samples.length);
      var lastsum = 0D;
      if (lastakg != null) {
        lastsum = lastakg._2
      }
      println("predictTime=" + i + ".sum=" + akg._2 + ".lastsum=" + lastsum);
      samples.foreach(f => { f.setSample(false); });
      var j = 0;
      newscenters.foreach(f => {
        f.setSample(true);
        samples(j) = f;
        j += 1;
      });
      if (lastakg == null) {
        lastakg = akg;
      } else if (akg._2 < lastakg._2) {
        lastakg = akg;
      } else if (akg._2 == lastakg._2) {
        equalTime += 1
        if (equalTime > 10) {
          tag = false;
        }
      } else if (akg._2 > lastakg._2) {
        greaterTime += 1
        if (greaterTime > 10) {
          tag = false;
        }
      }
    }
    lastakg
  }

}