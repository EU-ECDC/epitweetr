package org.ecdc.twitter 

import org.apache.spark.sql.{SparkSession, Dataset}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.{unix_timestamp, col}
import demy.storage.Storage
import scala.collection.JavaConverters._
import demy.util.{log => l, util}

case class StringIterator(it:Iterator[String]) {
  def hasNext = it.hasNext
  def theNext = it.next
} 

object JavaBridge {
  def getSparkSession(cores:Int = 0) = {
    val spark = 
      SparkSession.builder()
        .master(s"local[${if(cores == 0) "*" else cores.toString}]")
        .appName("epitweetr")
        //.config("spark.driver.memory", "4g")
        //.config("spark.executor.memory", "2g")
        .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    spark
  }
  def getSparkStorage(spark:SparkSession) = Storage.getSparkStorage

  def df2StdOut(df:Dataset[_], minPartitions:Int = 200):Unit  = 
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
      .get
      .foreach(println _)

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
