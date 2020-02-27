package org.ecdc.twitter 

import demy.mllib.linalg.implicits._
import demy.storage.{Storage, WriteMode, FSNode}
import demy.util.{log => l, util}
import demy.{Application, Configuration}
import org.apache.spark.sql.{SparkSession, Column, Dataset, Row}
import org.apache.spark.sql.functions.{col, lit, coalesce, lower, concat}
import scala.{Iterator => It}

object Geolocation {
  def localize(tweetDir:String) = {
    implicit val spark = SparkSession.builder().master("local[*]").appName("twitter geolocation").getOrCreate()
    import spark.implicits._
    spark.sparkContext.setLogLevel("WARN")
    implicit val storage = demy.storage.Storage.getSparkStorage
    val tweets = spark.read.json(tweetDir)
    tweets.show
    l.msg("hello demy!!")
    tweets
    
  }

}
