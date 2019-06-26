package cn.spark.study.mlib.akg

import java.util.Random
import scala.math.pow
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
 

class KMeansObject extends Logging with Serializable  {

  //2维平面点
  case class D2Point(x: Int, y: Int);

  // 计算两个的点距离的平方(欧氏距离) 
  // 公式  x1 - x2 的平方  + y1 - y2的平方 ，的结果集再开根. 
  
  def distanceSquared(p1: D2Point, p2: D2Point):Double = {
     val distance:Double = pow((p1.x - p2.x).doubleValue(), 2) + pow((p1.y - p2.y).doubleValue(), 2)
     Math.sqrt(distance)
  }

  //
  // 计算一群点中距离某个点最近的点的角标
  def closestPoint(p: D2Point, points: Array[D2Point]): Int = {
    var bestIndex = 0
    var closest: Double = 0D
    for (i <- points.indices) {
      val dist = distanceSquared(p, points(i))
      if (dist < closest) {
        closest = dist
        bestIndex = i
      }
    }
    bestIndex
  }

  //计算两个点的和
  def addPoints(p1: D2Point, p2: D2Point): D2Point = {
    new D2Point(p1.x + p2.x, p1.y + p2.y)
  }

  def main(args: Array[String]): Unit = {
    //其它参数，通过spark-submit启动的时候，从参数文件获取
    val conf = new SparkConf().setAppName("KMeansObject").setMaster("local[2]");
    val sc = new SparkContext(conf);
    val seed =System.currentTimeMillis();
    val rand = new Random(seed);
    val points: RDD[D2Point] = sc.parallelize((1 to 1000), 1).map(f => new D2Point(f + rand.nextInt(100), f + rand.nextInt(100)));
    // points 后面再
    val K: Int = 4; // 获取集合是4的
    //获取样本数据
    val kPoints: Array[D2Point] = points.takeSample(false, K, seed);
    kPoints.foreach(println);
    //临时数据，记录每个新中心点与旧中心的距离平方值的和
    var tempDist: Double = Double.PositiveInfinity;
    // 阈值 
    val convergeDist = 0.8;
   while (tempDist > convergeDist) {
       // 找到距离每个点最近的点的角标，并记录(index, (p, 1))
        val closest: RDD[(Int, (D2Point, Int))] = points.map(p => (closestPoint(p, kPoints), (p, 1)))
        // 根据角标，聚合周围最近的点，并把周围的点相加
        val pointStats = closest.reduceByKey { case ((point1: D2Point, n1: Int), (point2: D2Point, n2: Int)) => (addPoints(point1, point2), n1 + n2) }
        // 计算周围点的新中心点
        val newPoints   = pointStats.map { case (i, (point, n)) => (i, new D2Point(point.x / n, point.y / n)) }.collectAsMap()
        // 累加每个新中心点与旧中心的距离平方值
        tempDist = 0.0
        val dPoint = new D2Point(1,1)
        for (i <- 0 until K) {
            val t = newPoints.getOrElse(i, dPoint);
            if ( t == dPoint){
               println("i="+i);
            }
            tempDist += distanceSquared(kPoints(i),t );
        }
        println("Distance between iterations: " + tempDist)
        // 将旧中心点替换为新中心点
        for (i <- 0 until K) {
             val t = newPoints.getOrElse(i, dPoint);
            if ( t == dPoint){
               println("i="+i);
            }
            kPoints(i) = t;
        }
    }
     // 打印最终的中心点
    println("Final K points: ");
    kPoints.foreach(println);
  }
 
   

}