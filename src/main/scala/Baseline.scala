/**
  * Baseline for hackaton
  */


import breeze.numerics.abs
import org.apache.hadoop.io.compress.GzipCodec
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.mllib.classification.LogisticRegressionWithLBFGS
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

import scala.Array._
import scala.collection.mutable.ArrayBuffer

case class PairWithCommonFriends(person1: Int, person2: Int, commonFriendsCount: Int)
case class UserFriends(user: Int, friends: Array[Int])
case class AgeSex(age: Int, sex: Int)

object Baseline {

  def main(args: Array[String]) {

    val sparkConf = new SparkConf().setAppName("Baseline")
    val sc = new SparkContext(sparkConf)
    val sqlc = new SQLContext(sc)

    import sqlc.implicits._
    val dataDir = if (args.length == 1) args(0) else "./"

    val graphPath = dataDir + "trainGraph"
    val reversedGraphPath = dataDir + "trainSubReversedGraph"
    val commonFriendsPath = dataDir + "commonFriendsCountsPartitioned"
    val demographyPath = dataDir + "demography"
    val predictionPath = dataDir + "prediction"
    val modelPath = dataDir + "LogisticRegressionModel"
    val numPartitions = 200
    val numPartitionsGraph = 1100

    // read graph, flat and reverse it
    // step 1.a from description

    val graph = {
      sc.textFile(graphPath)
        .map(line => {
          val lineSplit = line.split("\t")
          val user = lineSplit(0).toInt
          val friends = {
            lineSplit(1)
              .replace("{(", "")
              .replace(")}", "")
              .split("\\),\\(")
              .map(t => t.split(",")(0).toInt)
          }
          UserFriends(user, friends)
        })
    }

    graph
      .filter(userFriends => userFriends.friends.length >= 8 && userFriends.friends.length <= 1000)
      .flatMap(userFriends => userFriends.friends.map(x => (x, userFriends.user)))
      .groupByKey(numPartitions)
      .map(t => UserFriends(t._1, t._2.toArray))
      .map(userFriends => userFriends.friends.sorted)
      .filter(friends => friends.length >= 2 && friends.length <= 2000)
      .map(friends => new Tuple1(friends))
      .toDF
      .write.parquet(reversedGraphPath)

    // for each pair of ppl count the amount of their common friends
    // amount of shared friends for pair (A, B) and for pair (B, A) is the same
    // so order pair: A < B and count common friends for pairs unique up to permutation
    // step 1.b

    def generatePairs(pplWithCommonFriends: Seq[Int], numPartitions: Int, k: Int) = {
      val pairs = ArrayBuffer.empty[(Int, Int)]
      for (i <- 0 until pplWithCommonFriends.length) {
        if (pplWithCommonFriends(i) % numPartitions == k) {
          for (j <- 0 until pplWithCommonFriends.length) {
            if (i != j) {
              pairs.append((pplWithCommonFriends(i), pplWithCommonFriends(j)))
            }
          }
        }
      }
      pairs
    }


    //val graphParts = 0 +: range(7, numPartitions - 1, 11)
    val graphParts = 0 +: range(7, 8, 11)

    for (k <- graphParts) {
      val commonFriendsCounts = {
        sqlc.read.parquet(reversedGraphPath)
          .map(t => generatePairs(t.getAs[Seq[Int]](0), numPartitionsGraph, k))
          .flatMap(pair => pair.map(x => x -> 1))
          .reduceByKey((x, y) => x + y)
          .map(t => PairWithCommonFriends(t._1._1, t._1._2, t._2))
          .filter(pair => pair.commonFriendsCount > 8)
      }

      commonFriendsCounts.toDF.repartition(4).write.parquet(commonFriendsPath + "/part_" + k)
    }

    // prepare data for training model
    // step 2

    // хотим ли мы вот тут еще меньше для регрессии?

    val commonFriendsCounts = {
      sqlc
        .read.parquet(commonFriendsPath + "/part_0")
        .map(t => PairWithCommonFriends(t.getAs[Int](0), t.getAs[Int](1), t.getAs[Int](2)))
        .sample(false, 0.05)
    }


    // step 3
    val usersBC = sc.broadcast(graph.map(userFriends => userFriends.user).collect().toSet)

    val positives = {
      graph
        .flatMap(
          userFriends => userFriends.friends
            .filter(x => usersBC.value.contains(x))
            .map(x => (userFriends.user, x) -> 1.0)
        )
    }

    // step 4
    val ageSex = {
      sc.textFile(demographyPath)
        .map(line => {
          val lineSplit = line.trim().split("\t")
          if (lineSplit(2) == "") {
            (lineSplit(0).toInt -> AgeSex(0, lineSplit(3).toInt))
          }
          else {
            (lineSplit(0).toInt -> AgeSex(lineSplit(2).toInt, lineSplit(3).toInt))
          }
        })
    }

    val ageSexBC = sc.broadcast(ageSex.collectAsMap())

    // step 5
    def prepareData(
                     commonFriendsCounts: RDD[PairWithCommonFriends],
                     positives: RDD[((Int, Int), Double)],
                     ageSexBC: Broadcast[scala.collection.Map[Int, AgeSex]]) = {

      commonFriendsCounts
        .map(pair => (pair.person1, pair.person2) -> (Vectors.dense(
          pair.commonFriendsCount.toDouble,
          abs(ageSexBC.value.getOrElse(pair.person1, AgeSex(0, 0)).age - ageSexBC.value.getOrElse(pair.person2, AgeSex(0, 0)).age).toDouble,
          if (ageSexBC.value.getOrElse(pair.person1, AgeSex(0, 0)).sex == ageSexBC.value.getOrElse(pair.person2, AgeSex(0, 0)).sex) 1.0 else 0.0))
        )
        .leftOuterJoin(positives)
    }

    val data = {
      prepareData(commonFriendsCounts, positives, ageSexBC)
      .map(t => LabeledPoint(t._2._2.getOrElse(0.0), t._2._1))
    }


    // split data into training (10%) and validation (90%)
    // step 6
    val splits = data.randomSplit(Array(0.1, 0.9), seed = 11L)
    val training = splits(0).cache()

    // run training algorithm to build the model
    val model = {
      new LogisticRegressionWithLBFGS()
        .setNumClasses(2)
        .run(training)
    }

    model.clearThreshold()

    // estimate model quality

    val threshold = 0

    // compute scores on the test set
    // step 7
    val testCommonFriendsCounts = {
      sqlc
        .read.parquet(commonFriendsPath + "/part_*/")
        .map(t => PairWithCommonFriends(t.getAs[Int](0), t.getAs[Int](1), t.getAs[Int](2)))
        .filter(pair => pair.person1 % 11 == 7 || pair.person2 % 11 == 7)
    }

    val testData = {
      prepareData(testCommonFriendsCounts, positives, ageSexBC)
        .map(t => t._1 -> LabeledPoint(t._2._2.getOrElse(0.0), t._2._1))
        .filter(t => t._2.label == 0.0)
    }

    // step 8
    val testPrediction = {
      testData
        .flatMap { case (id, LabeledPoint(label, features)) =>
          val prediction = model.predict(features)
          Seq(id._1 -> (id._2, prediction))
        }
        .filter(t => t._1 % 11 == 7 && t._2._2 >= threshold)
        .groupByKey(numPartitions)
        .map(t => {
          val user = t._1
          val firendsWithRatings = t._2
          val topBestFriends = firendsWithRatings.toList.sortBy(-_._2).take(100).map(x => x._1)
          (user, topBestFriends)
        })
        .sortByKey(true, 1)
        .map(t => t._1 + "\t" + t._2.mkString("\t"))
    }

    testPrediction.saveAsTextFile(predictionPath,  classOf[GzipCodec])

  }
}
