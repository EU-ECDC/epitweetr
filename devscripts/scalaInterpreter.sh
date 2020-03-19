#!/bin/bash
export SPARK_VERSION=2.4.4
export SBT_OPTS="-Xmx6G -XX:+UseConcMarkSweepGC -XX:+CMSClassUnloadingEnabled -Xss2M  -Duser.timezone=GMT"
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
send "val french = Language(name= \"french\", code = \"fr\", vectorsPath = \"/home/fod/datapub/semantic/cc.fr.300.vec.gz\")\r"
interact'
cd ..
