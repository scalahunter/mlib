package cn.spark.study.mlib.movie

import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.recommendation.ALS
import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.recommendation.ALSModel
import org.apache.spark.sql.DataFrame
import cn.spark.study.mlib.akg.Config

 

object MovieLens {
  
  //表结构 用户id,电影id,评分值, 时间
  case class Rating(userId: Int, movieId: Int, rating: Float, timestamp: Long)
  
  def parseRating(str: String): Rating = {
     val fields = str.split(",")
     assert(fields.size == 4)
     Rating(fields(0).toInt, fields(1).toInt, fields(2).toFloat, fields(3).toLong)
  }
  
  def main(args: Array[String]) {
    val spark = SparkSession
      .builder
      .appName("ALSExample")
      .master("local[2]")
      .getOrCreate()
    import spark.implicits._

    // $example on$
    val ratings = spark.read.textFile(Config.CUR_PRJ_DOC+"//sample_movielens_ratings.txt").map(parseRating).toDF()
    //将数据集切分为训练集和测试集
    val Array(training, test) = ratings.randomSplit(Array(0.8, 0.2))

    //使用ALS在训练集数据上构建推荐模型
    val als = new ALS().setMaxIter(5).setRegParam(0.01).setUserCol("userId").setItemCol("movieId").setRatingCol("rating")
    val model: ALSModel = als.fit(training)

    // 通过计算rmse(均方根误差)来评估模型
    //为确保不获取到NaN评估参数，我们将冷启动策略设置为drop。
    val predictions = model.transform(test)

    val evaluator = new RegressionEvaluator().
                setMetricName("rmse").
                setLabelCol("rating").
                setPredictionCol("prediction")
    val rmse = evaluator.evaluate(predictions)
    println(s"Root-mean-square error = $rmse")

    //每个用户推荐的前十个电影
    val userRecs:DataFrame  = model.recommendForAllUsers(10)
    //每个电影推荐的十个用户
 
    val movieRecs :DataFrame = model.recommendForAllItems(10)
    println("========================userRecs========================\n");
    userRecs.show()
    movieRecs.show()
    //movieRecs.rdd.saveAsTextFile(Config.CUR_PRJ_DOC+"/movieRecs.txt")
    spark.stop()
  }
  
  /*
 https://mp.weixin.qq.com/s?__biz=MzA3MDY0NTMxOQ==&mid=2247484291&idx=1&sn=4599b4e31c2190e363aa379a92794ace&chksm=9f38e0aba84f69bd5b78b48e31b3f5b3792ec40e2d25fdbe6bc735f9c98ceb4584462b08e439&scene=21#wechat_redirect
 学习网站 
 https://movielens.org/
 +-------+--------------------+
|movieId|     recommendations|
+-------+--------------------+
|      1|[[9, 8.200775], [...|
|      3|[[5, 9.599541], [...|
|      2|[[9, 8.798586], [...|
+-------+--------------------+   
    
   * */
}