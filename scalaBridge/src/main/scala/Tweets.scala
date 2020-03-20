package org.ecdc.twitter 


import demy.mllib.linalg.implicits._
import demy.mllib.index.implicits._
import demy.storage.{Storage, WriteMode, FSNode}
import demy.util.{log => l, util}
import demy.{Application, Configuration}
import org.apache.spark.sql.{SparkSession, Column, Dataset, Row}
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.{col, udf, input_file_name, explode, coalesce, when, lit, concat}
import java.sql.Timestamp
import demy.mllib.text.Word2VecApplier

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
      Tweets.getSearchJson(path)
        .select(
          udf((p:String)=>p.split("/").reverse(2)).apply(input_file_name()).as("topic")
          , udf((p:String)=>p.split("/").reverse(0)).apply(input_file_name()).as("file")
          , explode(col("statuses")).as("tweet")
        )
        .select(
          col("topic")
          , col("file")
          , col("tweet.id")
          , col("tweet.text")
          , coalesce(col("tweet.retweeted_status.text"), col("tweet.quoted_status.text")).as("linked_text")
          , col("tweet.user.description").as("user_description")
          , coalesce(col("tweet.retweeted_status.user.description"), col("tweet.quoted_status.user.description")).as("linked_user_description")
          , col("tweet.retweeted_status").isNotNull.as("is_retweet")
          , col("tweet.user.screen_name")
          , col("tweet.user.id").as("user_id") 
          , col("tweet.user.location").as("user_location") 
          , coalesce(col("tweet.retweeted_status.user.location"), col("tweet.quoted_status.user.location")).as("linked_user_location")
          , col("tweet.created_at")
          , col("tweet.lang")
          , coalesce(col("tweet.retweeted_status.lang"), col("tweet.quoted_status.lang")).as("linked_lang")
          , col("tweet.coordinates.coordinates").getItem(0).as("longitude")
          , col("tweet.coordinates.coordinates").getItem(1).as("latitude")
          , col("tweet.place.place_type")
          , col("tweet.place.name").as("place_name")
          , when(col("tweet.place.country_code") === lit("US"), concat(col("tweet.place.full_name"), lit(" "), col("tweet.place.country"))).otherwise(col("tweet.place.full_name")).as("place_full_name")
          , col("tweet.place.country_code").as("place_country_code")
          , col("tweet.place.country").as("place_country")
          , ((col("tweet.place.bounding_box.coordinates").getItem(0).getItem(0).getItem(0)
             + col("tweet.place.bounding_box.coordinates").getItem(0).getItem(1).getItem(0)
             + col("tweet.place.bounding_box.coordinates").getItem(0).getItem(2).getItem(0)
             + col("tweet.place.bounding_box.coordinates").getItem(0).getItem(3).getItem(0)
             )/4).as("place_longitude")
          , ((col("tweet.place.bounding_box.coordinates").getItem(0).getItem(0).getItem(1)
             + col("tweet.place.bounding_box.coordinates").getItem(0).getItem(1).getItem(1)
             + col("tweet.place.bounding_box.coordinates").getItem(0).getItem(2).getItem(1)
             + col("tweet.place.bounding_box.coordinates").getItem(0).getItem(3).getItem(1)
             )/4).as("place_latitude")
        ) 
  }

  def getJsonTweetsVecorized(path:String, langs:Seq[Language], reuseIndex:Boolean = true, indexPath:String)(implicit spark:SparkSession, storage:Storage) = {
    val textLang =  
      Map(
        "text"->Some("lang")
        , "linked_text"->Some("linked_lang")
        , "user_description"->Some("lang")
        , "linked_user_description"->Some("linked_lang")
        , "linked_user_location"->None
        , "user_location"->None
        , "place_full_name"->None
      )
          
    val vectors =  langs.map(l => l.getVectorsDF().select(concat(col("word"), lit("LANG"), col("lang")).as("word"), col("vector"))).reduce(_.union(_))

    textLang.foldLeft(Tweets.getJsonTweets(path)){case (df, (textCol, oLang)) => 
      oLang match {
        case Some(lang) =>
          df.luceneLookup(right = vectors
            , query = udf((text:String, lang:String)=>
                if(text == null) 
                  Array[String]() 
                else 
                  text
                    .split("((http|https|HTTP|HTTPS|ftp|FTP)://(\\S)+|[^\\p{L}]|(?<!(^|[A-Z]))(?=[A-Z])|(?<!^)(?=[A-Z][a-z]))+")
                    .filter(_.size > 0)
                    .map(w => s"${w}LANG$lang")
               ).apply(col(textCol), col(lang))
            , popularity = None
            , text = col("word")
            , rightSelect= Array(col("vector"))
            , leftSelect= Array(col("*"))
            , maxLevDistance = 0
            , indexPath = indexPath
            , reuseExistingIndex = reuseIndex
            , indexPartitions = 1
            , maxRowsInMemory=1
            , indexScanParallelism = 2
            , tokenizeText = false
            , caseInsensitive = false
            , minScore = 0.0
            , boostAcronyms=false
            , strategy="demy.mllib.index.StandardStrategy"
          ).withColumnRenamed("array", s"${textCol}_vec")
        case _ => df
      }
    }
  }
}
