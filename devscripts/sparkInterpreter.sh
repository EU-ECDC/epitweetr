#!/bin/bash
export SPARK_VERSION=2.4.5
export SBT_OPTS="-Xmx16G -XX:+UseConcMarkSweepGC -XX:+CMSClassUnloadingEnabled -Xss2M  -Duser.timezone=GMT"
if [ -z ${EPI_HOME+x} ]; then echo "please set EPI_HOME is unset"; exit 1; fi
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
send "val geonames = Geonames(source = \"'$EPI_HOME'/geo/allCountries.txt\", destination = \"'$EPI_HOME'/geo\", simplify = true)\r"
send "val french = Language(name= \"French\", code = \"fr\", vectorsPath = \"'$EPI_HOME'/languages/fr.txt.gz\")\r"
send "val english = Language(name= \"English\", code = \"en\", vectorsPath = \"'$EPI_HOME'/languages/en.txt.gz\")\r"
send "val spanish = Language(name= \"Spanish\", code = \"es\", vectorsPath = \"'$EPI_HOME'/languages/es.txt.gz\")\r"
send "val portuguese = Language(name= \"Portuguese\", code = \"pt\", vectorsPath = \"'$EPI_HOME'/languages/pt.txt.gz\")\r"
send "val langs = Seq(portuguese, spanish, english, french)\r"
send "val langIndex = '$EPI_HOME'/geo/lang_vectors.index\r"
send "val pathFilter = Seq(\".*2020\\\\.03\\\\.18.*\", \".*2020\\\\.03\\\\.19.*\", \".*2020\\\\.03\\\\.20.*\", \".*2020\\\\.03\\\\.21.*\", \".*2020\\\\.03\\\\.22.*\", \".*2020\\\\.03\\\\.23.*\", \".*2020\\\\.03\\\\.24.*\", \".*2020\\\\.03\\\\.25.*\", \".*2020\\\\.03\\\\.26.*\", \".*2020\\\\.03\\\\.27.*\", \".*2020\\\\.03\\\\.28.*\", \".*2020\\\\.03\\\\.29.*\")\r"
send "//val df = Tweets.getTweets(tweetPath = \"'$EPI_HOME'/tweets/search\", geoPath = \"'$EPI_HOME'/tweets/geolocated\", pathFilter = pathFilter, columns = Seq(\"id\", \"topic\"), langs=Seq(french, spanish, english), parallelism = Some(8) )\r"
interact'

cd ..
