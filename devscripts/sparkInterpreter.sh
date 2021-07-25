#!/bin/bash
export SPARK_VERSION=3.0.3
export SBT_OPTS="-Xmx16G -XX:+UseConcMarkSweepGC -XX:+CMSClassUnloadingEnabled -Xss2M  -Duser.timezone=GMT"
if [ -z ${EPI_HOME+x} ]; then echo "please set EPI_HOME is unset"; exit 1; fi
cd scala
expect -c '
spawn sbt console
expect "scala>"
send "import org.ecdc.epitweetr.Settings\r"
send "implicit val conf = Settings.apply(\"'$EPI_HOME'\")\r"
send "conf.load\r"
send "import org.ecdc.epitweetr.fs.LuceneActor\r"
send "import org.apache.spark.sql.SparkSession\r"
send "import org.apache.spark.sql.functions._\r"
send "org.ecdc.epitweetr.API.run(conf.epiHome)\r"
interact'

cd ..
