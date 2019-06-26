package cn.spark.study.mlib.akg
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import cn.spark.study.mlib.Lang.AKGCase
import cn.spark.study.mlib.Lang.Point

object AKGTest {

  class AKGStudentScore(_name: String, point: Point) extends AKGCase(point) {
    val name = _name
    def toString0(): String = {
      "id:" + id + ",name:" + name + ",point.x:" + point.x + ",sample:" + isSample()
    }
  }

  def main(args: Array[String]): Unit = {
    //因为这里开了两个线程，所以在运行map的时候是有两个线程在跑的.
    val conf = new SparkConf().setAppName("AKGTest").setMaster("local[2]");
    val sc = new SparkContext(conf);
    val counterAcc = sc.accumulator[Int](0)
    val lines: RDD[String] = sc.textFile(Config.CUR_PRJ_GIT + "/student_score_chinese.csv");
    val rdd: RDD[AKGStudentScore] = lines.map(f => {
      val items = f.split(",")
      // println("map:"+Thread.currentThread().getName);
      counterAcc.add(1)
      new AKGStudentScore(items(0), new Point(items(1).toDouble, items(2).toDouble));
    });
    //这里随便遍历代码。就可以把rdd的数据都抽到Driver本地，把Array换成RDD
    rdd.foreach(e => { if (e.id != 1) {} })
    //这代码放在这里正确，会成功打印，但是如果没放到这里打印失败.
    println("counterAcc forecha===" + counterAcc.value)
    //这里一当数据量大的，不得了。要把所有在各container的数据，都拉取到本地Drvier进行运行。
    //所以在源数据层，给每一条数据
    val arrays = rdd.collect();
    val akg = new AKG[AKGStudentScore](sc, arrays);
    akg.init()
    if (true) {
    //  return ;
    }
    // 天才(99-100),优秀(95左右),良好(85左右)，中等(75左右) ，刚合格(65左右)，差点合格(58左右)
    // 不合格(50左右)，笨蛋(10,20)
    val samples = akg.takeSamples(10);
    val akgType = akg.predict(samples, 100);
    for (akg0 <- akgType._1) {
      val items: Array[Int] = akg0.ids
      println("============================================");
      println("level:" + akg0.sample.getLevel() + ",sum:" + akg0.sum + ",len:" + akg0.idsLen + "");
      for (item <- items) {
        println(arrays(item - 1).toString0());
      }
    }

  }

}