package org.ecdc.twitter 

import demy.mllib.linalg.implicits._
import demy.mllib.index.implicits._
import demy.storage.{Storage, WriteMode, FSNode}
import demy.util.{log => l, util}
import demy.{Application, Configuration}
import org.apache.spark.sql.{SparkSession, Column, Dataset, Row, DataFrame}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.{col, udf, input_file_name, explode, coalesce, when, lit, concat, struct, expr}
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
      //, "linked_user_description"->Some("linked_lang")
      , "user_location"->Some("lang")
      //, "linked_user_location"->Some("linked_lang")
      , "place_full_name"->None.asInstanceOf[Option[String]]
      , "linked_place_full_name"->None.asInstanceOf[Option[String]]
    )
  def getSearchJson(path:String, parallelism:Option[Int]=None, pathFilter:Option[String]=None)(implicit spark:SparkSession, storage:Storage) = {
    val files = storage.getNode(path).list(recursive = true).filter(n => n.path.endsWith(".json.gz") && pathFilter.map(f => n.path.matches(f)).getOrElse(true)).map{n => n.path}.toArray
    Some(spark.read.option("timestampFormat", "EEE MMM dd HH:mm:ss ZZZZZ yyyy").schema(schemas.searchAPI)
      .json(files: _*)
      .withColumn("topic", udf((p:String)=>p.split("/").reverse(2)).apply(input_file_name()))
      .withColumn("file",  udf((p:String)=>p.split("/").reverse(0)).apply(input_file_name()))
    )
    .map(df => parallelism match {case Some(p) => df.repartition(p) case _ => df})
    .get 
  }
  
  def getSearchBlocks(path:String, parallelism:Option[Int]=None, pathFilter:Option[String]=None)(implicit spark:SparkSession, storage:Storage) = {
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

  def getJsonTweets(path:String, parallelism:Option[Int]=None, pathFilter:Option[String]=None)(implicit spark:SparkSession, storage:Storage)= {
      def bboxAvg(base:String, i:Int) = 
        (col(s"$base.place.bounding_box.coordinates").getItem(0).getItem(0).getItem(i)
          + col(s"$base.place.bounding_box.coordinates").getItem(0).getItem(1).getItem(i)
          + col(s"$base.place.bounding_box.coordinates").getItem(0).getItem(2).getItem(i)
          + col(s"$base.place.bounding_box.coordinates").getItem(0).getItem(3).getItem(i)
          )/4
      def fullPlace(base:String) = when(col(s"$base.place.country_code") === lit("US"), concat(col(s"$base.place.full_name"), lit(" "), col(s"$base.place.country"))).otherwise(col(s"$base.place.full_name"))

      Tweets.getSearchJson(path = path, parallelism = parallelism, pathFilter = pathFilter)
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
          , coalesce(col("tweet.retweeted_status.coordinates.coordinates").getItem(0), col("tweet.quoted_status.coordinates.coordinates").getItem(0)).as("linked_longitude")
          , coalesce(col("tweet.retweeted_status.coordinates.coordinates").getItem(1), col("tweet.quoted_status.coordinates.coordinates").getItem(1)).as("linked_latitude")
          , col("tweet.place.place_type")
          , col("tweet.place.name").as("place_name")
          , fullPlace("tweet").as("place_full_name")
          , coalesce(fullPlace("tweet.retweeted_status"), fullPlace("tweet.quoted_status")).as("linked_place_full_name")
          , col("tweet.place.country_code").as("place_country_code")
          , col("tweet.place.country").as("place_country")
          , bboxAvg("tweet", 0).as("place_longitude")
          , bboxAvg("tweet", 1).as("place_latitude")
          , coalesce(bboxAvg("tweet.retweeted_status", 0), bboxAvg("tweet.quoted_status", 0)).as("linked_place_longitude")
          , coalesce(bboxAvg("tweet.retweeted_status", 1), bboxAvg("tweet.quoted_status", 1)).as("linked_place_latitude")

        )
  }

  def getJsonTweetsVectorized(path:String, langs:Seq[Language], reuseIndex:Boolean = true, indexPath:String, parallelism:Option[Int]=None, pathFilter:Option[String]=None)(implicit spark:SparkSession, storage:Storage) = {
    val vectors =  langs.map(l => l.getVectorsDF().select(concat(col("word"), lit("LANG"), col("lang")).as("word"), col("vector"))).reduce(_.union(_))
    val twitterSplitter = "((http|https|HTTP|HTTPS|ftp|FTP)://(\\S)+|[^\\p{L}]|@+|#+|(?<!(^|[A-Z]))(?=[A-Z])|(?<!^)(?=[A-Z][a-z]))+"
    val simpleSplitter = "\\s+"
    Some(Tweets.getJsonTweets(path = path, parallelism = parallelism, pathFilter = pathFilter)
      .where(col("lang").isin(langs.map(l => l.code):_*))
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


  def getJsonTweetWithLocations(path:String, langs:Seq[Language], geonames:Geonames, reuseIndex:Boolean = true, indexPath:String, minScore:Int, parallelism:Option[Int]=None, pathFilter:Option[String]=None)
    (implicit spark:SparkSession, storage:Storage) = {
     val twitterSplitter = "((http|https|HTTP|HTTPS|ftp|FTP)://(\\S)+|[^\\p{L}]|@+|#+|(?<!(^|[A-Z]))(?=[A-Z])|(?<!^)(?=[A-Z][a-z]))+"
     Some(
       this.getJsonTweetsVectorized(path = path, langs = langs, reuseIndex = reuseIndex, indexPath = indexPath, parallelism = parallelism, pathFilter = pathFilter)
         .addLikehoods(
           languages = langs
           , geonames = geonames
           , vectorsColNames = textLangCols.toSeq.flatMap{case (textCol, oLangCol) => oLangCol.map(langCol => s"${textCol}_vec")}
           , langCodeColNames = textLangCols.toSeq.flatMap{case (textCol, oLangCol) => oLangCol}
           , likehoodColNames  = textLangCols.toSeq.flatMap{case (textCol, oLangCol) => oLangCol.map(langCol => s"${textCol}_geolike")}
        )
        .withColumnRenamed("longitude", "tweet_longitude")
        .withColumnRenamed("latitude", "tweet_latitude")
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
    )
    .map(df => 
      df.toDF(df.schema.map(f => if(f.name.endsWith("_res")) f.name.replace("_res", "_loc") else f.name):_* )
    )
    .get
  }

  def extractLocations(sourcePath:String, destPath:String, langs:Array[Language], geonames:Geonames, reuseIndex:Boolean, indexPath:String, minScore:Int, parallelism:Int, spark:SparkSession, storage:Storage):Unit
    =  {
      implicit val s = spark
      implicit val ss = storage
      extractLocations(sourcePath= sourcePath, destPath=destPath, langs = langs.toSeq, geonames=geonames, reuseIndex=reuseIndex, indexPath=indexPath, minScore=minScore, parallelism=Some(parallelism))
    }
  def extractLocations(sourcePath:String, destPath:String, langs:Seq[Language], geonames:Geonames, reuseIndex:Boolean = true, indexPath:String, minScore:Int, parallelism:Option[Int]=None) 
    (implicit spark:SparkSession, storage:Storage):Unit = {
    storage.ensurePathExists(destPath)
    val hoursDone = storage.getNode(destPath)
      .list(recursive = true)
      .filter(n => n.path.endsWith(".json.gz"))
      .map(_.path).map(p => p.split("/").reverse)
      .map(p => (p(0).replace(".json.gz", "")))
      .toSet

    val hours = storage.getNode(sourcePath)
      .list(recursive = true)
      .filter(n => n.path.endsWith(".json.gz"))
      .map(_.path).map(p => p.split("/").reverse)
      .map(p => (p(0).replace(".json.gz", "")))
      .filter(p => !hoursDone(p))
      .distinct
      .sortWith(_ < _)
      .dropRight(1)
      .toArray

    hours.foreach{hour => 
      l.msg(s"geolocating tweets for $hour") 
      val df = Tweets.getJsonTweetWithLocations(
          path = sourcePath
          , langs=langs
          , geonames = geonames
          , reuseIndex = reuseIndex
          , indexPath=indexPath
          , minScore = minScore
          , parallelism = parallelism
          , pathFilter = Some(s".*${hour.replace(".", "\\.")}.*")
        )
        .select((
          Seq(col("topic"), col("file"), col("lang"), col("id"))
            ++ textLangCols.keys.toSeq
                .map(c => s"${c}_loc")
                .map(c => when(col(s"$c.geo_id").isNotNull, struct(Seq("geo_id","geo_name", "geo_code","geo_type","geo_country_code", "geo_longitude", "geo_latitude").map(cc => col(s"$c.$cc")):_*)).as(c))
          ):_*
        )
        .withColumn("is_geo_located"
          , when(textLangCols.keys.toSeq.map(c => col(s"${c}_loc").isNull).reduce( _ && _)
           , false
           ).otherwise(true)
        )
      df.write.mode("overwrite").options(Map("compression" -> "gzip"))
        .json(s"$destPath/${hour.take(7)}/$hour.json.gz")
      df.unpersist
    }
  }

  def getTweets(tweetPath:String, geoPath:String, pathFilter:Array[String], columns:Array[String], langs:Array[Language],  parallelism:Int,  spark:SparkSession, storage:Storage):DataFrame =  
  {
    implicit val s = spark
    implicit val ss = storage
    getTweets(tweetPath:String, geoPath:String, pathFilter = pathFilter.toSeq, columns = columns.toSeq, langs.toSeq,  parallelism = Some(parallelism))
  }
      
  def getTweets(tweetPath:String, geoPath:String, pathFilter:Seq[String], columns:Seq[String], langs:Seq[Language],  parallelism:Option[Int])
    (implicit spark:SparkSession, storage:Storage):DataFrame = {
      Some(
        pathFilter.map{filter =>
          val tweets = getJsonTweets(path = tweetPath, parallelism = parallelism, pathFilter = Some(filter))
          val locationFiles = storage.getNode(geoPath).list(recursive = true).filter(n => n.path.endsWith(".json.gz") && n.path.matches(filter)).map{n => n.path}
        
          Some((tweets
                , spark.read.schema(schemas.geoLocatedSchema).json(locationFiles: _*))
            )
            .map{case (tweets, locations) => 
               ( tweets
                  .dropDuplicates("id", "topic")
                  .where(col("lang").isin(langs.map(_.code):_*))
                ,locations
                  .dropDuplicates("id", "topic")
                  .where(col("lang").isin(langs.map(_.code):_*))
                  .drop("topic", "file", "lang")
                  .withColumnRenamed("id", "lid")
               )
            }
            .map{ case (tweets, locations) => 
              tweets
                .join(locations, tweets("id")===locations("lid"), "left")
                .drop("lid")
            }
            .get
        }
        .reduce{_.union(_)}
      )
      .map(df => 
        df.dropDuplicates("id", "topic")
          .select(columns.map(c => expr(c)):_*)
      )
      .get
    }
}
