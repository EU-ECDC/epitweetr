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
  def main(args: Array[String]): Unit = {
    val cmd = Map(
      "getTweets" -> Set("tweetPath", "geoPath", "pathFilter", "columns", "langCodes", "langNames", "langPaths",  "parallelism")
      , "extractLocations" -> Set("sourcePath", "destPath", "langCodes", "langNames", "langPaths", "geonamesSource", "geonamesDestination", "reuseIndex", "indexPath", "minScore", "parallelism")
    )
    if(args == null || args.size < 3 || !cmd.contains(args(0)) || args.size %2 == 0 ) 
      l.msg(s"first argument must be within ${cmd.keys} and followed by a set of 'key' 'values' parameters, but the command ${args(0)} is followed by ${args.size -1} values")
    else {
      val command = args(0)
      val params = Seq.range(0, (args.size -1)/2).map(i => (args(i*2 + 1), args(i*2 + 2))).toMap
      if(!cmd(command).subsetOf(params.keySet))
        l.msg(s"Cannot run $command, expected named parameters are ${cmd(command)}")
      else if(command == "getTweets") {
         implicit val spark = JavaBridge.getSparkSession(params.get("cores").map(_.toInt).getOrElse(0)) 
         implicit val storage = JavaBridge.getSparkStorage(spark) 
         Some(
           getTweets(
             tweetPath = params.get("tweetPath").get
             , geoPath = params.get("geoPath").get
             , pathFilter = params.get("pathFilter").get.split(",")
             , columns = params.get("columns").get.split("\\|\\|")
             , langs = 
                 Some(Seq("langCodes", "langNames", "langPaths"))
                   .map{s => s.map(p => params(p).split(",").map(_.trim))}
                   .map{case Seq(codes, names, paths) =>
                     codes.zip(names.zip(paths))
                       .map{case (code, (name, path)) =>
                          Language(name = name, code = code, vectorsPath = path)
                       }
                   }
                   .get
             , parallelism = params.get("parallelism").map(_.toInt).get
             , spark = spark
             , storage = storage
           )
         ).map(df => JavaBridge.df2StdOut(df, minPartitions = params.get("parallelism").map(_.toInt).get))
      } else if(command == "extractLocations"){ 
         implicit val spark = JavaBridge.getSparkSession(params.get("cores").map(_.toInt).getOrElse(0)) 
         implicit val storage = JavaBridge.getSparkStorage(spark) 
         extractLocations(
           sourcePath =  params.get("sourcePath").get
           , destPath = params.get("destPath").get
           , langs = 
               Some(Seq("langCodes", "langNames", "langPaths"))
                 .map{s => s.map(p => params(p).split(",").map(_.trim))}
                 .map{case Seq(codes, names, paths) =>
                   codes.zip(names.zip(paths))
                     .map{case (code, (name, path)) =>
                        Language(name = name, code = code, vectorsPath = path)
                     }
                 }
                 .get
           , geonames = Geonames( params.get("geonamesSource").get,  params.get("geonamesDestination").get)
           , reuseIndex = params.get("reuseIndex").map(_.toBoolean).get
           , indexPath = params.get("indexPath").get
           , minScore = params.get("minScore").map(_.toInt).get
           , parallelism = params.get("parallelism").map(_.toInt).get
           , spark = spark
           , storage = storage
         )
      } else {
        throw new Exception("Not implemented @epi") 
      }
    } 
  }
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

  def getJsonTweetsVectorized(path:String, langs:Seq[Language], reuseIndex:Boolean = true, indexPath:String, parallelism:Option[Int]=None, pathFilter:Option[String]=None, ignoreIdsOn:Option[String]=None)(implicit spark:SparkSession, storage:Storage) = {
    val vectors =  langs.map(l => l.getVectorsDF().select(concat(col("word"), lit("LANG"), col("lang")).as("word"), col("vector"))).reduce(_.union(_))
    val twitterSplitter = "((http|https|HTTP|HTTPS|ftp|FTP)://(\\S)+|[^\\p{L}]|@+|#+|(?<!(^|[A-Z]))(?=[A-Z])|(?<!^)(?=[A-Z][a-z]))+|RT"
    val simpleSplitter = "\\s+"
    Some(
      (Tweets.getJsonTweets(path = path, parallelism = parallelism, pathFilter = pathFilter)
          .where(col("lang").isin(langs.map(l => l.code):_*))
       , ignoreIdsOn
      )
    )
    .map{
      case (df, None) => df
      case (df, Some(toIgnorePath)) =>
        Some((df.join(spark.read.schema(schemas.toIgnoreSchema).json(toIgnorePath).select(col("id").as("ignore_id")).distinct(), col("id")===col("ignore_id"), "left")
                .where(col("ignore_id").isNotNull)
                .drop("ignore_id")
              ,parallelism
             )
          )
          .map{
            case (df, Some(p)) =>
              df.coalesce(p)
            case (df, None) =>
              df
          }.get
    }
    .map(df => df
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


  def getJsonTweetWithLocations(path:String, langs:Seq[Language], geonames:Geonames, reuseIndex:Boolean = true, indexPath:String, minScore:Int, parallelism:Option[Int]=None, pathFilter:Option[String]=None, ignoreIdsOn:Option[String]=None)
    (implicit spark:SparkSession, storage:Storage) = {
     val twitterSplitter = "((http|https|HTTP|HTTPS|ftp|FTP)://(\\S)+|[^\\p{L}]|@+|#+|(?<!(^|[A-Z]))(?=[A-Z])|(?<!^)(?=[A-Z][a-z]))+|RT"
     Some(
       this.getJsonTweetsVectorized(path = path, langs = langs, reuseIndex = reuseIndex, indexPath = indexPath, parallelism = parallelism, pathFilter = pathFilter, ignoreIdsOn = ignoreIdsOn)
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
    val filesDone = storage.getNode(destPath)
      .list(recursive = true)
      .filter(n => n.path.endsWith(".json.gz") && n.isDirectory)
      .map(_.path).map(p => p.split("/").reverse)
      .map(p => (p(0).replace(".json.gz", "")))
      .distinct
      .sortWith(_ < _)
    
    val doneButLast = filesDone.dropRight(1).toSet
    val lastDone = filesDone.last
   
    val files = storage.getNode(sourcePath)
      .list(recursive = true)
      .filter(n => n.path.endsWith(".json.gz"))
      .map(_.path).map(p => p.split("/").reverse)
      .map(p => (p(0).replace(".json.gz", "")))
      .filter(p => !doneButLast(p))
      .distinct
      .sortWith(_ < _)
      .toArray

    files.foreach{file => 
      l.msg(s"geolocating tweets for $file") 
      val df = Tweets.getJsonTweetWithLocations(
          path = sourcePath
          , langs=langs
          , geonames = geonames
          , reuseIndex = reuseIndex
          , indexPath=indexPath
          , minScore = minScore
          , parallelism = parallelism
          , pathFilter = Some(s".*${file.replace(".", "\\.")}.*")
          , ignoreIdsOn = if(file == lastDone) Some(s"$destPath/${file.take(7)}/$file.json.gz") else None
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
      df.write.mode("append").options(Map("compression" -> "gzip"))
        .json(s"$destPath/${file.take(7)}/$file.json.gz")
      df.unpersist
    }
    l.msg(s"geolocation finished successfully") 

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
