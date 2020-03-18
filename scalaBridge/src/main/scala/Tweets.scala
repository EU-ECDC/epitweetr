package org.ecdc.twitter 


import demy.mllib.linalg.implicits._
import demy.storage.{Storage, WriteMode, FSNode}
import demy.util.{log => l, util}
import demy.{Application, Configuration}
import org.apache.spark.sql.{SparkSession, Column, Dataset, Row}
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.{col, udf, input_file_name}
import java.sql.Timestamp


object Tweets {
  def getSearchJson(path:String)(implicit spark:SparkSession, storage:Storage) = {
    spark.read.option("timestampFormat", "EEE MMM dd HH:mm:ss ZZZZZ yyyy").schema(schemas.searchAPI)
      .json(storage.getNode(path).list(recursive = true).filter(_.path.endsWith(".json.gz")).map(_.path):_*)
  }
  
  def getSearchBlocks(path:String)(implicit spark:SparkSession, storage:Storage) = {
     Tweets.getSearchJson(path)
       .select(
         col("search_metadata")
         , udf((p:String)=>p.split("/").reverse(2)).apply(input_file_name()).as("topic")
         , udf((seq:Seq[Row]) => 
           (
             seq.map(r => r.getAs[Timestamp]("created_at")).reduceOption((a, b)=>if(a.before(b)) a else b)
             , seq.map(r => r.getAs[java.sql.Timestamp]("created_at")).reduceOption((a, b)=>if(a.after(b)) a else b)
             , seq.size)
           ).apply(col("statuses")).as("metrics")
         )
       .select(col("search_metadata.query"), col("topic"), col("metrics.*"))
       .toDF("query", "topics", "from", "to", "count")
  }

  def getJsonTweets(path:String)(implicit spark:SparkSession, storage:Storage)= {
  
  }
  
}
