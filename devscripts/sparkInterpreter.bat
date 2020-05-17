#!/bin/bash
set HADOOP_HOME="c:/Users/pancho/github/ecdc-twitter-tool/epitweetr/inst"
set PATH=%PATH%;%HADOOP_HOME%\bin
set SPARK_VERSION=2.4.5
set SBT_OPTS="-Xmx6G"
cd scalaBridge
sbt console
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
implicit val spark = SparkSession.builder().master("local[*]").appName("test").getOrCreate()
import spark.implicits._
import demy.util.util
spark.sparkContext.setLogLevel("WARN")
import org.ecdc.twitter._
implicit val storage = demy.storage.Storage.getSparkStorage
import demy.Configuration
import demy.storage.Storage
implicit val s = Storage.getLocalStorage
val geonames = Geonames(source = "c:/tools/ecdc_data/geo/allCountries.txt", destination = "c:/tools/ecdc_data/geo")
val french = Language(name= "French", code = "fr", vectorsPath = "C:\tools\ecdc_data\languages\fr.txt.gz")
val english = Language(name= "English", code = "en", vectorsPath = "C:\tools\ecdc_data\languages\en.txt.gz")
val spanish = Language(name= "Spanish", code = "es", vectorsPath = "C:\tools\ecdc_data\languages\es.txt.gz")
val pathFilter = Seq(".*2020\\\\.03\\\\.18.*", ".*2020\\\\.03\\\\.19.*", ".*2020\\\\.03\\\\.20.*", ".*2020\\\\.03\\\\.21.*", ".*2020\\\\.03\\\\.22.*", ".*2020\\\\.03\\\\.23.*", ".*2020\\\\.03\\\\.24.*", ".*2020\\\\.03\\\\.25.*", ".*2020\\\\.03\\\\.26.*", ".*2020\\\\.03\\\\.27.*", ".*2020\\\\.03\\\\.28.*", ".*2020\\\\.03\\\\.29.*")
//val df = Tweets.getTweets(tweetPath = "/home/fod/github/ecdc-twitter-tool/data/tweets/search", geoPath = "/home/fod/github/ecdc-twitter-tool/data/tweets/geo", pathFilter = pathFilter, columns = Seq("id", "topic"), langs=Seq(french, spanish, english), parallelism = Some(8) )
interact'

cd ..
