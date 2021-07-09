#!/bin/bash
export SPARK_VERSION=3.1.2
export SBT_OPTS="-Xmx10G -XX:+UseConcMarkSweepGC -XX:+CMSClassUnloadingEnabled -Xss2M  -Duser.timezone=GMT"
cd scala
expect -c '
spawn sbt consoleQuick
expect "scala>"
send "import org.apache.spark.sql.SparkSession\r"
send "import org.apache.spark.sql.functions._\r"
send "implicit val spark = SparkSession.builder().master(\"local\[*\]\").appName(\"test\").getOrCreate()\r"
send "import spark.implicits._\r"
send "spark.sparkContext.setLogLevel(\"WARN\")\r"
interact'

cd ..
