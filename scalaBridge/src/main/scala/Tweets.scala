package org.ecdc.twitter 


import demy.mllib.linalg.implicits._
import demy.mllib.index.implicits._
import demy.storage.{Storage, WriteMode, FSNode}
import demy.util.{log => l, util}
import demy.{Application, Configuration}
import org.apache.spark.sql.{SparkSession, Column, Dataset, Row}
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.{col, udf, input_file_name, explode, coalesce, when, lit, concat, struct}
import java.sql.Timestamp
import demy.mllib.text.Word2VecApplier
import Language.AddLikehood
import Geonames.GeoLookup
 
object Tweets {
  val textLangCols =  
    Map(
      "text"->Some("lang")
      , "linked_text"->Some("linked_lang")
      , "user_description"->Some("lang")
      , "linked_user_description"->Some("linked_lang")
      , "user_location"->Some("lang")
      , "linked_user_location"->Some("linked_lang")
      , "place_full_name"->None.asInstanceOf[Option[String]]
    )
  def getSearchJson(path:String, parallelism:Option[Int]=None)(implicit spark:SparkSession, storage:Storage) = {
    Some(spark.read.option("timestampFormat", "EEE MMM dd HH:mm:ss ZZZZZ yyyy").schema(schemas.searchAPI)
      .json(storage.getNode(path).list(recursive = true).filter(_.path.endsWith(".json.gz")).map(_.path):_*)
      .withColumn("topic", udf((p:String)=>p.split("/").reverse(2)).apply(input_file_name()))
      .withColumn("file",  udf((p:String)=>p.split("/").reverse(0)).apply(input_file_name()))
    )
    .map(df => parallelism match {case Some(p) => df.repartition(p) case _ => df})
    .get 
  }
  
  def getSearchBlocks(path:String, parallelism:Option[Int]=None)(implicit spark:SparkSession, storage:Storage) = {
     Tweets.getSearchJson(path = path, parallelism = parallelism)
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

  def getJsonTweets(path:String, parallelism:Option[Int]=None)(implicit spark:SparkSession, storage:Storage)= {
      
    util.checkpoint(
      Tweets.getSearchJson(path = path, parallelism = parallelism)
        .select(
          col("topic")
          , col("file")
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
       , s"/home/fod/deleteme/test2.parquet"
       , partitionBy = None
       , reuseExisting = false
       )
  }

  def getJsonTweetsVectorized(path:String, langs:Seq[Language], reuseIndex:Boolean = true, indexPath:String, parallelism:Option[Int]=None)(implicit spark:SparkSession, storage:Storage) = {
    val vectors =  langs.map(l => l.getVectorsDF().select(concat(col("word"), lit("LANG"), col("lang")).as("word"), col("vector"))).reduce(_.union(_))
    val twitterSplitter = "((http|https|HTTP|HTTPS|ftp|FTP)://(\\S)+|[^\\p{L}]|@+|#+|(?<!(^|[A-Z]))(?=[A-Z])|(?<!^)(?=[A-Z][a-z]))+"
    val simpleSplitter = "\\s+"
    Some(Tweets.getJsonTweets(path = path, parallelism = parallelism)
      .luceneLookups(right = vectors
        , queries = textLangCols.toSeq.flatMap{
            case (textCol, Some(lang)) => 
              Some(
                udf((text:String, lang:String) =>
                  if(text == null) 
                    Array[String]() 
                  else 
                    text.replaceAll(twitterSplitter, " ")
                      .split(simpleSplitter)
                      .filter(_.size > 0)
                      .map(w => s"${w}LANG$lang")
                 ).apply(col(textCol), col(lang)).as(textCol)
               )
            case _ => None
          }
        , popularity = None
        , text = col("word")
        , rightSelect= Array(col("vector"))
        , leftSelect= Array(col("*"))
        , maxLevDistance = 0
        , indexPath = indexPath
        , reuseExistingIndex = reuseIndex
        , indexPartitions = 1
        , maxRowsInMemory=1
        , indexScanParallelism = 1
        , tokenizeRegex = None
        , caseInsensitive = false
        , minScore = 0.0
        , boostAcronyms=false
        , strategy="demy.mllib.index.StandardStrategy"
    ))
    .map(df => 
      df.toDF(df.schema.map(f => if(f.name.endsWith("_res")) f.name.replace("_res", "_vec") else f.name):_* )
    )
    .get
  }


  def getJsonTweetWithLocations(path:String, langs:Seq[Language], geonames:Geonames, reuseIndex:Boolean = true, indexPath:String, minScore:Int, parallelism:Option[Int]=None)(implicit spark:SparkSession, storage:Storage) = {
     val twitterSplitter = "((http|https|HTTP|HTTPS|ftp|FTP)://(\\S)+|[^\\p{L}]|@+|#+|(?<!(^|[A-Z]))(?=[A-Z])|(?<!^)(?=[A-Z][a-z]))+"
     util.checkpoint(
       this.getJsonTweetsVectorized(path = path, langs = langs, reuseIndex = reuseIndex, indexPath = indexPath, parallelism = parallelism)
         .addLikehoods(
           languages = langs
           , geonames = geonames
           , vectorsColNames = textLangCols.toSeq.flatMap{case (textCol, oLangCol) => oLangCol.map(langCol => s"${textCol}_vec")}
           , langCodeColNames = textLangCols.toSeq.flatMap{case (textCol, oLangCol) => oLangCol}
           , likehoodColNames  = textLangCols.toSeq.flatMap{case (textCol, oLangCol) => oLangCol.map(langCol => s"${textCol}_geolike")}
         )
         .withColumnRenamed("longitude", "tweet_longitude")
         .withColumnRenamed("latitude", "tweet_latitude")
       , s"/home/fod/deleteme/test.parquet"
       , partitionBy = None
       , reuseExisting = false
       )
      .geoLookup(
        geoTexts = textLangCols.toSeq.map{case (textCol, oLang) => col(textCol)} 
        , geoNames = geonames
        , maxLevDistance= 0
        , termWeightsCols = textLangCols.toSeq.map{case (textCol, oLang) => oLang.map(lang => s"${textCol}_geolike")}
        , reuseExistingIndex = reuseIndex
        , minScore = minScore
        , nGram = 3
        , stopWords = langs.flatMap(l => l.getStopWords).toSet
        , tokenizeRegex = Some(twitterSplitter)
        )
    
  }
}
