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
send "val langIndex = \"'$EPI_HOME'/geo/lang_vectors.index\"\r"
send "import Geonames.Geolocate\r"
send "val tweetPath=\"'$EPI_HOME'/tweets/search/SARS/2020/2020.06.10.00001.json.gz\"\r" 
send "val texts = Tweets.getJsonTweets(path=tweetPath).select(\"text\", \"lang\").where(col(\"lang\").isin(langs.map(_.code):_*))\r"
send "//val geoloc = texts.geolocate(textLangCols = Map(\"text\"->Some(\"lang\")), minScore = 0, nGram = 3, tokenizerRegex = Tweets.twitterSplitter, langs = langs, geonames = geonames, langIndexPath = langIndex)\r"
send "//geoloc.select(\"text\", \"lang\", \"text_geolike\").map(r => (r.getAs\[String\](\"text\").split(Tweets.twitterSplitter).filter(_.size>0).zip(r.getAs\[Seq\[Double\]\](\"text_geolike\").map(BigDecimal(_).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble)))).show(100, false)\r"
send "//geoloc.select(\"text\", \"lang\", \"text_geolike\").map(r => (r.getAs\[String\](\"text\").split(Tweets.twitterSplitter).filter(_.size>0).zip(r.getAs\[Seq\[Double\]\](\"text_geolike\").map(BigDecimal(_).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble)))).as\[Seq\[(String, Double)\]\].map(s => (s.flatMap{case(word, score) if score > 0.75 => Some(word) case _ => None}, s.flatMap{case(word, score) if score < 0.75 => Some(word) case _ =>None})).toDF(\"Place\", \"Other\").show(50, false)\r"
interact'

cd ..
