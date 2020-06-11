package org.ecdc.twitter 

import org.apache.spark.sql.{SparkSession, Dataset}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.{unix_timestamp, col}
import demy.storage.Storage
import scala.collection.JavaConverters._
import java.io.{BufferedWriter, OutputStreamWriter}
import demy.util.{log => l, util}
import org.apache.log4j.Logger
import org.apache.log4j.Level


case class StringIterator(it:Iterator[String]) {
  def hasNext = it.hasNext
  def theNext = it.next
} 

object JavaBridge {
  def getSparkSession(cores:Int = 0) = {
    Logger.getLogger("org.apache.spark").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    val spark = 
      SparkSession.builder()
        .master(s"local[${if(cores == 0) "*" else cores.toString}]")
        .config("spark.sql.files.ignoreCorruptFiles", true)
        .appName("epitweetr")
        .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    spark
  }
  def getSparkStorage(spark:SparkSession) = Storage.getSparkStorage

  def df2StdOut(df:Dataset[_]):Unit  = { 
    val out = new OutputStreamWriter(System.out, "UTF-8")
    val ls = System.getProperty("line.separator")
    Some(df)
      .map(df => df.select(df.schema.map{
          case StructField(name, LongType, _, _) => col(name).cast(StringType) 
          case StructField(name, TimestampType, _, _) => unix_timestamp(col(name)).as(name)
          case StructField(name, _, _, _) => col(name) 
        }:_*)
      )
      .map(df => df.toJSON.toLocalIterator.asScala)
      .get
      .foreach{line => 
        out.write(line, 0, line.length)
        out.write(ls, 0, ls.length)
        out.flush()
      }
  }
  def df2JsonIterator(df:Dataset[_], chunkSize:Int = 1000, minPartitions:Int = 200):StringIterator  = 
    Some(df)
      .map(df => df.select(df.schema.map{
          case StructField(name, LongType, _, _) => col(name).cast(StringType) 
          case StructField(name, TimestampType, _, _) => unix_timestamp(col(name)).as(name)
          case StructField(name, _, _, _) => col(name) 
        }:_*)
      )
      .map{df =>
        val numPart = df.rdd.getNumPartitions 
        if(numPart < minPartitions) {
          l.msg(s"increasing partitions from $numPart to $minPartitions before export")
          df.repartition(minPartitions)
        } else {
          df  
        }
      }
      .map(df => df.toJSON.toLocalIterator.asScala)
      .map(it => 
        it.map{ json =>
          val sb = StringBuilder.newBuilder
          sb.append("[")
          sb.append(json)
          it.take(chunkSize -1).foreach{oJson => 
            sb.append(",")
            sb.append(oJson)
          }
          sb.append("]")
          sb.toString
        }
      )
      .map(it => StringIterator(it))
      .get
}
