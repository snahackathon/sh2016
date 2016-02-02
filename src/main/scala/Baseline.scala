/**
  * Baseline for hackaton
  */


import breeze.numerics.abs
import org.apache.hadoop.io.compress.GzipCodec
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.mllib.classification.{LogisticRegressionModel, LogisticRegressionWithLBFGS}
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

object Baseline {

  def main(args: Array[String]) {

    val sparkConf = new SparkConf()
      .setAppName("Baseline")
    val sc = new SparkContext(sparkConf)
    val sqlc = new SQLContext(sc)

    import sqlc.implicits._

    val DATA_DIR = "./"

    val GRAPH = DATA_DIR + "trainGraph"
    val REVERSED_GRAPH = DATA_DIR + "trainSubReversedGraph"
    val COMMON_FRIENDS = DATA_DIR + "commonFriendsCountsPartitioned"
    val SOC_DEM = DATA_DIR + "anonimDetailsActive"
    val PREDICTION = DATA_DIR + "prediction"
    val MODEL_HOME = DATA_DIR + "LogisticRegressionModel"
    val NUM_PARTITIONS = 200
    val NUM_PARTITIONS_GRAPH = 107


    // read graph, flat and reverse it
    // step 1.a from description

    val graph = {
      sc.textFile(GRAPH)
        .map(line => {
          val lineSplit = line.split("\t")
          lineSplit(0).toInt -> lineSplit(1)
            .replace("{(", "")
            .replace(")}", "")
            .split("\\),\\(")
            .map(t => t.split(",")(0).toInt)
        })
        .filter(t => t._2.length >= 8 && t._2.length <= 1000)
    }

    graph
      .flatMap(t => t._2.map(x => (x, t._1.toInt)))
      .groupByKey(NUM_PARTITIONS)
      .map(t => t._2.toArray.sorted)
      .filter(t => t.length >= 2 && t.length <= 2000)
      .map(t => new Tuple1(t))
      .toDF
      .write.parquet(REVERSED_GRAPH)

    // for each couple of ppl count the amount of their common friends
    // amount of shared friends for couple (A, B) and for couple (B, A) is the same
    // so order couple : A < B and count common friends for couples unique up to permutation
    // step 1.b

    def generatePairs(pplWithCommonFriends: Seq[Int], numPartitions: Int, k: Int) = {
      val pairs = ArrayBuffer.empty[(Int, Int)]
      for (i <- 0 until pplWithCommonFriends.length) {
        if (pplWithCommonFriends(i) % numPartitions == k) {
          for (j <- i + 1 until pplWithCommonFriends.length) {
            pairs.append((pplWithCommonFriends(i), pplWithCommonFriends(j)))
          }
        }
      }
      pairs
    }

    for (k <- 0 until NUM_PARTITIONS_GRAPH) {
      val commonFriendsCounts = {
        sqlc.read.parquet(REVERSED_GRAPH)
          .map(t => generatePairs(t.getAs[Seq[Int]](0), NUM_PARTITIONS_GRAPH, k))
          .flatMap(t => t.map(x => x -> 1))
          .reduceByKey((x, y) => x + y)
          .filter(t => t._2 >= 8)
      }

      commonFriendsCounts.toDF.write.parquet(COMMON_FRIENDS + "/part_" + k)
    }

    // prepare data for training model
    // step 2
    

    val commonFriendsCounts = {
      sqlc
        .read.parquet(COMMON_FRIENDS + "/part_33")
        .map(t => (t.getAs[Row](0).getInt(0), t.getAs[Row](0).getInt(1)) -> t.getAs[Int](1))
    }

    // step 3
    val usersBC = sc.broadcast(graph.map(t => t._1).collect().toSet)

    val positives = {
      graph
        .flatMap(
          t => t._2
            .filter(x => (usersBC.value.contains(x) && x > t._1))
            .map(x => (t._1, x) -> 1.0)
        )
    }

    // step 4
    val ageSex = {
      sc.textFile(SOC_DEM)
        .map(line => {
          val lineSplit = line.trim().split("\t")
          if (lineSplit(2) == "") {
            (lineSplit(0).toInt ->(0, lineSplit(3).toInt))
          }
          else {
            (lineSplit(0).toInt ->(lineSplit(2).toInt, lineSplit(3).toInt))
          }
        })
    }

    val ageSexBC = sc.broadcast(ageSex.collectAsMap())

    // step 5
    def prepareData(
                     commonFriendsCounts: RDD[((Int, Int), Int)],
                     positives: RDD[((Int, Int), Double)],
                     ageSexBC: Broadcast[scala.collection.Map[Int, (Int, Int)]]) = {
      commonFriendsCounts
        .map(t => t._1 -> (Vectors.dense(
          t._2.toDouble,
          abs(ageSexBC.value.getOrElse(t._1._1, (0, 0))._1 - ageSexBC.value.getOrElse(t._1._2, (0, 0))._1).toDouble,
          if (ageSexBC.value.getOrElse(t._1._1, (0, 0))._2 == ageSexBC.value.getOrElse(t._1._2, (0, 0))._2) 1.0 else 0.0))
        )
        .leftOuterJoin(positives)
    }

    val data = {
      prepareData(commonFriendsCounts, positives, ageSexBC)
        .map(t => LabeledPoint(t._2._2.getOrElse(0.0), t._2._1))
    }


    // split data into training (1%) and validation (99%)
    // step 6
    val splits = data.randomSplit(Array(0.1, 0.9), seed = 11L)
    val training = splits(0).cache()
    val validation = splits(1)

    // run training algorithm to build the model
    val model = {
      new LogisticRegressionWithLBFGS()
        .setNumClasses(2)
        .run(training)
    }

    model.clearThreshold()
    model.save(sc, MODEL_HOME)

    val predictionAndLabels = {
      validation.map { case LabeledPoint(label, features) =>
        val prediction = model.predict(features)
        (prediction, label)
      }
    }

    // estimate model quality
    @transient val metricsLogReg = new BinaryClassificationMetrics(predictionAndLabels, 100)
    val threshold = metricsLogReg.fMeasureByThreshold(2.0).sortBy(-_._2).take(1)(0)._1

    val rocLogReg = metricsLogReg.areaUnderROC()
    println("model ROC = " + rocLogReg.toString)

    val sameModel = LogisticRegressionModel.load(sc, MODEL_HOME)

    // compute scores on the test set
    // step 7
    val testCommonFriendsCounts = {
      sqlc
        .read.parquet(COMMON_FRIENDS + "/part_*/")
        .map(t => (t.getAs[Row](0).getInt(0), t.getAs[Row](0).getInt(1)) -> t.getAs[Int](1))
        .filter(t => t._1._1 % 11 == 7 || t._1._2 % 11 == 7)
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
          Seq(id._1 ->(id._2, prediction), id._2 ->(id._1, prediction))
        }
        .filter(t => t._1 % 11 == 7 && t._2._2 >= threshold)
        .groupByKey(NUM_PARTITIONS)
        .map(t => t._1 -> t._2.take(100).toList.sortBy(-_._2).map(x => x._1))
        .sortByKey(true, 1)
        .map(t => t._1 + "\t" + t._2.mkString("\t"))
    }

    testPrediction.saveAsTextFile(PREDICTION,  classOf[GzipCodec])

  }
}
