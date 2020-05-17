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
import Language.LangTools
import Geonames.Geolocate
 
object Tweets {
  val twitterSplitter = "((http|https|HTTP|HTTPS|ftp|FTP)://(\\S)+|[^\\p{L}]|@+|#+|(?<!(^|[A-Z]))(?=[A-Z])|(?<!^)(?=[A-Z][a-z]))+|RT"
  def main(args: Array[String]): Unit = {
    val cmd = Map(
      "getTweets" -> Set("tweetPath", "geoPath", "pathFilter", "columns", "groupBy", "filterBy", "sortBy", "sourceExpressions", "langCodes", "langNames", "langPaths",  "parallelism")
      , "getSampleTweets" -> Set("tweetPath", "pathFilter", "langCodes", "langNames", "langPaths", "geonamesSource", "geonamesDestination", "langIndexPath", "limit")
      , "extractLocations" -> Set("sourcePath", "destPath", "langCodes", "langNames", "langPaths", "geonamesSource", "geonamesDestination", "langIndexPath", "minScore", "parallelism")
      , "updateGeonames" -> Set("geonamesSource", "geonamesDestination", "assemble", "index", "parallelism")
      , "updateLanguages" -> Set("langCodes", "langNames", "langPaths", "geonamesSource", "geonamesDestination", "langIndexPath", "parallelism")
    )
    if(args == null || args.size < 3 || !cmd.contains(args(0)) || args.size % 2 == 0 ) 
      l.msg(s"first argument must be within ${cmd.keys} and followed by a set of 'key' 'values' parameters, but the command ${args(0)} is followed by ${args.size -1} values")
    else {
      val command = args(0)
      val params = Seq.range(0, (args.size -1)/2).map(i => (args(i*2 + 1), args(i*2 + 2))).toMap
      if(!cmd(command).subsetOf(params.keySet))
        l.msg(s"Cannot run $command, expected named parameters are ${cmd(command)}")
      else if(command == "getSampleTweets") {
         implicit val spark = JavaBridge.getSparkSession(params.get("parallelism").map(_.toInt).getOrElse(0)) 
         implicit val storage = JavaBridge.getSparkStorage(spark) 
         Some(
           getSampleTweets(
             tweetPath = params.get("tweetPath").get
             , pathFilter = params.get("pathFilter")
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
             , langIndexPath = params.get("langIndexPath").get
             , limit = params.get("limit").get.toInt
           )
         ).map(df => JavaBridge.df2StdOut(df))
      } else if(command == "getTweets") {
         implicit val spark = JavaBridge.getSparkSession(params.get("parallelism").map(_.toInt).getOrElse(0)) 
         implicit val storage = JavaBridge.getSparkStorage(spark) 
         Some(
           getTweets(
             tweetPath = params.get("tweetPath").get
             , geoPath = params.get("geoPath").get
             , pathFilter = params.get("pathFilter").get.split(",").toSeq
             , columns = params.get("columns").get.split("\\|\\|").toSeq.map(_.trim).filter(_.size > 0)
             , groupBy = params.get("groupBy").get.split("\\|\\|").toSeq.map(_.trim).filter(_.size > 0)
             , filterBy = params.get("filterBy").get.split("\\|\\|").toSeq.map(_.trim).filter(_.size > 0)
             , sourceExpressions = 
                 params.get("sourceExpressions").get.split("\\|\\|\\|").map(_.trim).filter(_.size > 0).distinct
                   .map(tLine => 
                     Some(tLine.split("\\|\\|").toSeq.map(_.trim).filter(_.size > 0))
                       .map(s => (s(0), s.drop(1)))
                       .get
                    )
                   .toMap
             , sortBy = params.get("sortBy").get.split("\\|\\|").toSeq.map(_.trim).filter(_.size > 0)
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
             , parallelism = params.get("parallelism").map(_.toInt)
           )
         ).map(df => JavaBridge.df2StdOut(df, minPartitions = params.get("parallelism").map(_.toInt).get))

      } else if(command == "extractLocations"){ 
         implicit val spark = JavaBridge.getSparkSession(params.get("parallelism").map(_.toInt).getOrElse(0)) 
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
           , langIndexPath = params.get("langIndexPath").get
           , minScore = params.get("minScore").map(_.toInt).get
           , parallelism = params.get("parallelism").map(_.toInt)
         )
      } else if(command == "updateGeonames") {
         implicit val spark = JavaBridge.getSparkSession(params.get("parallelism").map(_.toInt).getOrElse(0)) 
         implicit val storage = JavaBridge.getSparkStorage(spark)
         import spark.implicits._
         val geonames = Geonames(params.get("geonamesSource").get, params.get("geonamesDestination").get)
         if(params.get("assemble").map(s => s.toBoolean).getOrElse(false))
           geonames.getDataset(reuseExisting = false)
         if(params.get("index").map(s => s.toBoolean).getOrElse(false)) {
           geonames.geolocateText(text=Seq("Viva Chile")).show
         }
      } else if(command == "updateLanguages"){ 
         implicit val spark = JavaBridge.getSparkSession(params.get("parallelism").map(_.toInt).getOrElse(0)) 
         implicit val storage = JavaBridge.getSparkStorage(spark) 
         Language.updateLanguages(
           langs = 
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
           , indexPath = params.get("langIndexPath").get
           , parallelism = params.get("parallelism").map(_.toInt)
         )
      } else {
        throw new Exception("Not implemented @epi") 
      }
    } 
  }
  val defaultTextLangCols =  
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
    val files = 
      storage.getNode(path).list(recursive = true)
        .map{n => n.path.replace("\\", "/")}
        .filter(p => p.endsWith(".json.gz") && pathFilter.map(f => p.matches(f)).getOrElse(true))
        .toArray
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
          , col("tweet.coordinates.coordinates").getItem(0).as("tweet_longitude")
          , col("tweet.coordinates.coordinates").getItem(1).as("tweet_latitude")
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

  def getJsonTweetsToGeolocate(
    path:String
    , langs:Seq[Language]
    , parallelism:Option[Int]=None
    , pathFilter:Option[String]=None
    , ignoreIdsOn:Option[String]=None
  )(implicit spark:SparkSession, storage:Storage) = {
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
                .where(col("ignore_id").isNull)
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
    .get
  }
  


  def extractLocations(
    sourcePath:String
    , destPath:String
    , langs:Seq[Language]
    , geonames:Geonames
    , langIndexPath:String
    , minScore:Int
    , parallelism:Option[Int]=None
    , textLangCols:Map[String, Option[String]] = defaultTextLangCols
    ) 
    (implicit spark:SparkSession, storage:Storage):Unit = {
    storage.ensurePathExists(destPath)
    val filesDone = storage.getNode(destPath)
      .list(recursive = true)
      .filter(n => n.path.endsWith(".json.gz") && n.isDirectory)
      .map(_.path.replace("\\", "/"))
      .map(p => p.split("/").reverse)
      .map(p => (p(0).replace(".json.gz", "")))
      .distinct
      .sortWith(_ < _)
    
    val doneButLast = filesDone match {
      case s if s.isEmpty => Set[String]()
      case s => s.dropRight(1).toSet
    }
    val lastDone = filesDone match {
      case s if s.isEmpty => None
      case s => Some(filesDone.last)
    }
 
   
    val files = storage.getNode(sourcePath)
      .list(recursive = true)
      .filter(n => n.path.endsWith(".json.gz"))
      .map(_.path.replace("\\", "/"))
      .map(p => p.split("/").reverse)
      .map(p => (p(0).replace(".json.gz", "")))
      .filter(p => !doneButLast(p))
      .distinct
      .sortWith(_ < _)
      .toArray

    files.foreach{file => 
      l.msg(s"geolocating tweets for $file") 
      Some(
        Tweets.getJsonTweetsToGeolocate(
          path = sourcePath
          , langs=langs
          , parallelism = parallelism
          , pathFilter = Some(s".*${file.replace(".", "\\.")}.*")
          , ignoreIdsOn = 
              lastDone
                .filter(ld => file == ld)
                .map(f => s"$destPath/${f.take(7)}/$file.json.gz")
          )
          .geolocate(
            textLangCols = textLangCols
            , minScore = minScore
            , maxLevDistance = 0
            , nGram = 3
            , tokenizerRegex = twitterSplitter
            , langs = langs 
            , geonames = geonames
            , reuseGeoIndex = true
            , langIndexPath=langIndexPath
            , reuseLangIndex = true
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
      )
      .map{df => 
        df.write.mode("append").options(Map("compression" -> "gzip"))
          .json(s"$destPath/${file.take(7)}/$file.json.gz")
        df.unpersist
      }
    }
    l.msg(s"geolocation finished successfully") 

  }

  def getTweets(
    tweetPath:String
    , geoPath:String
    , pathFilter:Seq[String]
    , columns:Seq[String]
    , groupBy:Seq[String]
    , filterBy:Seq[String]
    , sortBy:Seq[String]
    , sourceExpressions:Map[String, Seq[String]]
    , langs:Seq[Language]
    , parallelism:Option[Int]
    )(implicit spark:SparkSession, storage:Storage):DataFrame = {
      Some(
        pathFilter.map{filter =>
          val tweets = getJsonTweets(path = tweetPath, parallelism = parallelism, pathFilter = Some(filter))
          val locationFiles = 
            storage.getNode(geoPath)
              .list(recursive = true)
              .map(_.path.replace("\\", "/"))
              .filter(p => p.endsWith(".json.gz") && p.matches(filter))
          val geos = spark.read.schema(schemas.geoLocatedSchema).json(locationFiles: _*)
          val tweetsExpr = 
            sourceExpressions.get("tweet")
              .map(seq => seq.toSet.union(Set("id", "topic")).toSeq
                          .map(e => expr(e))
              )
              .getOrElse(Seq(col("*")))
          val geosExpr = 
            sourceExpressions.get("geo")
              .map(seq => seq.toSet.union(Set("id", "topic")).toSeq
                          .map(e => expr(e))
              )
              .getOrElse(Seq(col("*")))
          
          Some((tweets, geos))
            .map{case (tweets, geos) => 
               ( tweets
                  .where(col("lang").isin(langs.map(_.code):_*))
                  .select(tweetsExpr :_*)
                ,geos
                  .where(col("lang").isin(langs.map(_.code):_*))
                  .select(geosExpr :_*)
               )
            }
            .map{
              case (tweets, geos) if(filterBy.size == 0) => 
                (tweets, geos)  
              case (tweets, geos) => 
                (tweets.where(filterBy.map(w => expr(w)).reduce(_ && _)), geos)
            }
            .map{ case (tweets, geos) => 
              (tweets.dropDuplicates("id", "topic")
                , geos
                  .dropDuplicates("id", "topic")
                  .drop("topic", "file", "lang")
                  .withColumnRenamed("id", "lid")
              ) 
            }
            .map{ case (tweets, geos) => 
              tweets
                .join(geos, tweets("id")===geos("lid"), "left")
                .drop("lid")
            }
            .get
        }
        .reduce{_.union(_)}
      )
      .map(df => df.dropDuplicates("id", "topic"))
      .map{
        case df if(groupBy.size == 0) => 
          df.select(columns.map(c => expr(c)):_*) 
        case df => 
          df.groupBy(groupBy.map(gb => expr(gb)):_*)
            .agg(expr(columns.head), columns.drop(1).map(c => expr(c)):_*)
      }
      .map{
        case df if(sortBy.size == 0) => 
          df 
        case df =>
          df.orderBy(sortBy.map(ob => expr(ob)):_*)
      }
      .get
  }
  def getSampleTweets(
    tweetPath:String
    , pathFilter:Option[String]=None
    , langs:Seq[Language]
    , geonames:Geonames 
    , langIndexPath:String
    , limit:Int
  )(implicit spark:SparkSession, storage:Storage)  = {
    Tweets.getJsonTweets(path = tweetPath, pathFilter = pathFilter)
      .select("id", "text", "lang")
      .where(col("lang").isin(langs.map(l => l.code):_*))
      .geolocate(
        textLangCols = Map("text" ->Some("lang"))
        , maxLevDistance = 0
        , minScore = 0
        , nGram = 3
        , tokenizerRegex = Tweets.twitterSplitter
        , langs = langs
        , geonames = geonames
        , langIndexPath = langIndexPath
      )
      .select(
        col("id")
        , col("text")
        , col("lang")
        , col("geo_name")
        , col("geo_type")
        , col("geo_country_code")
        , udf((c:String) => if(c==null) null else c.split(",").head).apply(col("geo_country")).as("country")
        , col("_score_").as("score")
        , col("_tags_").as("tagged"))
      .limit(limit)    
    }
}
