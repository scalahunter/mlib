
package cn.spark.study.mlib.movie

import java.util.Random
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd._
import org.apache.spark.mllib.recommendation.{ ALS, Rating, MatrixFactorizationModel}
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import scala.Iterator
import cn.spark.study.mlib.akg.Config

object MovieLensALS {

  def main(args: Array[String]) {
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

    //  val sparkHome = "/zzti/libs/spark"
    val master = "local[2]"
    val conf = new SparkConf()
          .setMaster(master)
      .setAppName("MovieLensALS")
      .set("spark.executor.memory", "2g")

    //  System.setProperty("hadoop.home.dir", "H:\\大三\\spark\\winutils")
    val sc = new SparkContext(conf)

    ///movielens/medium/ratings.dat
    val ratings = sc.textFile(Config.CUR_PRJ_DOC +"/ratings.csv").filter(f=> !f.startsWith("userId")).map { line =>
      val fields = line.split(",")
      (fields(3).toInt % 10, Rating(fields(0).toInt, fields(1).toInt, fields(2).toDouble))
    }

    ///movielens/medium/movies.csv 
    // movie (id, movie_name)
    val movies = sc.textFile(Config.CUR_PRJ_DOC +"/movies.csv").filter(f=> !f.startsWith("movieId")).map { line =>
      val fields = line.split(",")
      (fields(0).toInt, fields(1))
    }.collect.toMap

    val numRatings = ratings.count
    val numUsers = ratings.map(_._2.user).distinct.count
    val numMovies = ratings.map(_._2.product).distinct.count
    println("Got " + numRatings + " ratings from "  + numUsers + " users on " + numMovies + " movies.")

    val mostRateMovieIds = ratings.map(_._2.product).countByValue()
      .toSeq
      .sortBy(-_._2)
      .take(50)
      .map(_._1)
      
    //获它们的id
    val random = new Random(0)
    val seclectedMovies = mostRateMovieIds.filter(x => random.nextDouble() < 0.2).map(x => (x, movies(x))).toSeq
    
    val myRatings = elicitateRatings(seclectedMovies)
    
    val myRatingsRDD = sc.parallelize(myRatings)

    val numPartitions = 20
   
    val training = ratings.filter(x => x._1 < 6).values
      .union(myRatingsRDD).repartition(numPartitions)
      .persist
    
    val validation = ratings.filter(x => x._1 >= 6 && x._1 < 8).values
      .repartition(numPartitions).persist

    val test = ratings.filter(x => x._1 >= 8).values.persist
    
    val numTraining = training.count
    
    val numValidation = validation.count
    
    val numTest = test.count
    println("Training:" + numTraining + ",validation: " + numValidation + ", test:" + numTest)
    
    val ranks = List(8, 12)
    val lambdas = List(0.1, 10.0)
    val numIters = List(10, 20)
    
    var bestModel: Option[MatrixFactorizationModel] = None
    var bestValidationRmse = Double.MaxValue
    var bestRank = 0
    var bestLambda = -1.0
    var bestNumIter = -1
    
    for (rank <- ranks; lambda <- lambdas; numIter <- numIters) {
      val model = ALS.train(training, rank, numIter, lambda)
      val validationRmse = computeRmse(model, validation, numValidation)
      println("RMSE (validation)=" + validationRmse + "for the model trained with rand =" + rank + ", lambda=" + lambda + ", and numIter= " + numIter + ".")
      if (validationRmse < bestValidationRmse) {
        bestModel = Some(model)
        bestValidationRmse = validationRmse
        bestRank = rank
        bestLambda = lambda
        bestNumIter = numIter
      }
    }
    val testRmse = computeRmse(bestModel.get, test, numTest)
    println("The best model was trained with rank=" + bestRank + " and lambda =" + bestLambda + ", and numIter =" + bestNumIter + ", and itsRMSE on the test set is" + testRmse + ".")
    val myRateMoviesIds = myRatings.map(_.product).toSet
    val candidates = sc.parallelize(movies.keys.filter(!myRateMoviesIds.contains(_)).toSeq)
    val recommendations = bestModel.get.predict(candidates.map((0, _)))
      .collect()
      .sortBy((-_.rating))
      .take(50)
    var i = 1
    println("movies recommended for you:")
    recommendations.foreach { r =>
      println("%2d".format(i) + ":" + movies(r.product))
      i += 1
     }

  }

  /** Compute RMSE (Root Mean Squared Error). */
  def computeRmse(model: MatrixFactorizationModel, data: RDD[Rating], n: Long) = {
    val predictions: RDD[Rating] = model.predict(data.map(x => (x.user, x.product)))
    val predictionsAndRatings = predictions.map(x => ((x.user, x.product), x.rating))
      .join(data.map(x => ((x.user, x.product), x.rating)))
      .values
    math.sqrt(predictionsAndRatings.map(x => (x._1 - x._2) * (x._1 - x._2)).reduce(_ + _) / n)
  }

  /** Elicitate ratings from command-line. */
  def elicitateRatings(movies: Seq[(Int, String)]) = {
    val prompt = "Please rate the following movie (1-5 (best), or 0 if not seen):"
    println(prompt)
    val ratings = movies.flatMap { x =>
      var rating: Option[Rating] = None
      var valid = false
      while (!valid) {
        print(x._2 + ": ")
        try {
          val r = Console.readInt
          if (r < 0 || r > 5) {
            println(prompt)
          } else {
            valid = true
            if (r > 0) {
              rating = Some(Rating(0, x._1, r))
            }
          }
        } catch {
          case e: Exception => println(prompt)
        }
      }
      rating match {
        case Some(r) => Iterator(r)
        case None    => Iterator.empty
      }
    }
    if (ratings.isEmpty) {
      error("No rating provided!")
    } else {
      ratings
    }

  }

}