#!/bin/bash
export SPARK_VERSION=2.4.5
export SBT_OPTS="-Xmx16G -XX:+UseConcMarkSweepGC -XX:+CMSClassUnloadingEnabled -Xss2M  -Duser.timezone=GMT"
cd scalaBridge
expect -c '
spawn sbt console
expect "scala>"
send "import org.apache.spark.sql.SparkSession\r"
send "import org.apache.spark.sql.functions._\r"
send "implicit val spark = SparkSession.builder().master(\"local\[*\]\").appName(\"test\").getOrCreate()\r"
send "import spark.implicits._\r"
send "import demy.util.util\r"
send "spark.sparkContext.setLogLevel(\"WARN\")\r"
send "import org.ecdc.twitter._\r"
send "implicit val storage = demy.storage.Storage.getSparkStorage\r"
send "import demy.Configuration\r"
send "import demy.storage.Storage\r"
send "implicit val s = Storage.getLocalStorage\r"
send "val geonames = Geonames(source = \"/home/fod/datapub/geo/allCountries.txt\", destination = \"/home/fod/github/ecdc-twitter-tool/data/geo\")\r"
send "val french = Language(name= \"French\", code = \"fr\", vectorsPath = \"/home/fod/datapub/semantic/cc.fr.300.vec.gz\")\r"
send "val english = Language(name= \"English\", code = \"en\", vectorsPath = \"/home/fod/datapub/semantic/cc.en.300.vec.gz\")\r"
send "val spanish = Language(name= \"Spanish\", code = \"es\", vectorsPath = \"/home/fod/datapub/semantic/cc.es.300.vec.gz\")\r"
send "val pathFilter = Seq(\".*2020\\\\.03\\\\.18.*\", \".*2020\\\\.03\\\\.19.*\", \".*2020\\\\.03\\\\.20.*\", \".*2020\\\\.03\\\\.21.*\", \".*2020\\\\.03\\\\.22.*\", \".*2020\\\\.03\\\\.23.*\", \".*2020\\\\.03\\\\.24.*\", \".*2020\\\\.03\\\\.25.*\", \".*2020\\\\.03\\\\.26.*\", \".*2020\\\\.03\\\\.27.*\", \".*2020\\\\.03\\\\.28.*\", \".*2020\\\\.03\\\\.29.*\")\r"
send "//val df = Tweets.getTweets(tweetPath = \"/home/fod/github/ecdc-twitter-tool/data/tweets/search\", geoPath = \"/home/fod/github/ecdc-twitter-tool/data/tweets/geo\", pathFilter = pathFilter, columns = Seq(\"id\", \"topic\"), langs=Seq(french, spanish, english), parallelism = Some(8) )\r"
interact'

cd ..
