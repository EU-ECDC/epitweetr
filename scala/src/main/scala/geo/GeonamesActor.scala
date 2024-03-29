package org.ecdc.epitweetr.geo

import org.ecdc.twitter.{Language}
import Geonames.Geolocate
import demy.util.{log => l, util}
import org.ecdc.epitweetr.{Settings, EpitweetrActor}
import org.ecdc.epitweetr.fs.{TextToGeo}
import akka.pattern.{ask, pipe}
import scala.concurrent.{Future, Await}
import scala.concurrent.duration._
import akka.actor.{ActorSystem, Actor, ActorLogging, Props}
import akka.pattern.ask
import akka.actor.ActorRef
import akka.Done
import akka.util.ByteString
import akka.util.{Timeout}
import scala.collection.JavaConverters._
import org.apache.spark.sql.{SparkSession, Column, Dataset, Row, DataFrame}
import org.apache.spark.sql.functions.{col, lit, array_join, udf}
import demy.storage.{Storage, FSNode}
import scala.concurrent.ExecutionContext
import scala.util.{Try, Success, Failure}
import java.nio.charset.StandardCharsets
import spray.json.JsonParser
import scala.util.Random

class GeonamesActor(conf:Settings) extends Actor with ActorLogging {
  implicit val executionContext = context.system.dispatchers.lookup("lucene-dispatcher")
  implicit val cnf = conf
  def receive = {
    case GeonamesActor.TrainingSetRequest(excludedLangs, locationSamples, jsonnl, caller) => 
      implicit val timeout: Timeout = conf.fsQueryTimeout.seconds //For ask property
      implicit val s = conf.getSparkSession
      implicit val st = conf.getSparkStorage
      import s.implicits._
      var sep = ""
      Future {
        if(!jsonnl) { 
          Await.result(caller ?  ByteString("[", ByteString.UTF_8), Duration.Inf)
        }
        val nonGeos =
          conf.languages
            .get
            .filterNot(l => excludedLangs.contains(l.code))
            .map{l => 
              l.getNonGeoVectors(conf.geonames).limit(500).select(col("word"), lit(false), lit(l.code)).as[(String, Boolean, String)]
            }
        val trainingData = if(locationSamples) {
          nonGeos ++ Seq(
            conf.geonames
                .getLocationSample(splitAliases=true, otherCitiesCount = 200)
                .map{loc => (loc, true, "all")}
          ) 
        } else {
          nonGeos
        }

        if(trainingData.size > 0) {
          trainingData
            .reduce(_.union(_))
            .toDF("word", "isLocation", "lang")
            .toJSON
            .toLocalIterator
            .asScala
            .foreach{line => 
               Await.result(caller ? ByteString(
                 s"${sep}${line.toString}\n", 
                 ByteString.UTF_8
               ), Duration.Inf)
               if(!jsonnl) sep = ","
            }
          }
        if(!jsonnl) { 
          Await.result(caller ?  ByteString("]", ByteString.UTF_8), Duration.Inf)
        }
        caller ! Done
      }.recover {
        case e: Exception => 
          caller ! ByteString(s"[Stream--error]: ${e.getCause} ${e.getMessage}: ${e.getStackTrace.mkString("\n")}", ByteString.UTF_8)
      }.onComplete { case  _ =>
      }
    case GeonamesActor.GeolocateTextsRequest(toGeo, minScore, forcedGeo, forcedGeoCodes, topics, jsonnl, caller) =>
      implicit val timeout: Timeout = conf.fsQueryTimeout.seconds //For ask property
      implicit val s = conf.getSparkSession
      implicit val st = conf.getSparkStorage
      import s.implicits._
      var sep = ""
      Future {
        if(!jsonnl) { 
          Await.result(caller ?  ByteString("[", ByteString.UTF_8), Duration.Inf)
        }
        implicit val s = conf.getSparkSession
        implicit val st = conf.getSparkStorage
        import s.implicits._
        val readyLangs = GeoTraining.trainedLanguages()
        if(conf.languages.size > readyLangs.size) println(s"only ${readyLangs.size} of ${conf.languages.size} are ready")
        val df0 = 
          toGeo.toDS
            .withColumn("lang", udf((lang:String) =>  if(lang == null || lang == "all") null else lang).apply(col("lang")))
        val df =  df0.geolocate(
              textLangCols = Map("text" -> (if(readyLangs.size > 0) Some("lang") else None)) 
              , maxLevDistance = 0
              , minScore = minScore.getOrElse(0)
              , nBefore = conf.geoNBefore
              , nAfter = conf.geoNAfter
              , tokenizerRegex = conf.splitter
              , langs = readyLangs
              , geonames = conf.geonames
              , reuseGeoIndex = true
              , langIndexPath=conf.langIndexPath
              , reuseLangIndex = true
              , forcedGeo = forcedGeo
              , forcedGeoCodes = forcedGeoCodes
              , closestTo = topics
             ).select(col("id"), col("text"), col("lang"), col("geo_code"), col("geo_country_code"), col("geo_country"), col("geo_name"), array_join(col("_tags_"), " ").as("tags"))
             .withColumn("geo_country", udf((country:String)=>if(country == null) null else country.split(",").head.trim).apply(col("geo_country")))
        //df.show
        df.toJSON
          .collect
          .foreach{line =>
             Await.result(caller ? ByteString(
               s"${sep}${line.toString}\n", 
               ByteString.UTF_8
             ), Duration.Inf)
             if(!jsonnl) sep = ","
          }
        if(!jsonnl) { 
          Await.result(caller ?  ByteString("]", ByteString.UTF_8), Duration.Inf)
        }
        caller ! Done
      }.recover {
        case e: Exception => 
          caller ! ByteString(s"[Stream--error]: ${e.getCause} ${e.getMessage}: ${e.getStackTrace.mkString("\n")}", ByteString.UTF_8)
      }.onComplete { case  _ =>
      }
    case GeonamesActor.TrainLanguagesRequest(trainingSet, jsonnl, caller) => 
      implicit val timeout: Timeout = conf.fsQueryTimeout.seconds //For ask property
      implicit val s = conf.getSparkSession
      implicit val st = conf.getSparkStorage
      import s.implicits._
      //Ensuring languages models are up to date
      Language.updateLanguageIndexes(conf.languages.get, conf.geonames, indexPath=conf.langIndexPath)


      var sep = ""
      Future {
        if(!jsonnl) { 
          Await.result(caller ?  ByteString("[", ByteString.UTF_8), Duration.Inf)
        }
        implicit val s = conf.getSparkSession
        implicit val st = conf.getSparkStorage
        import s.implicits._
        val readyLangs = GeoTraining.trainedLanguages()
        if(conf.languages.size > readyLangs.size) println(s"only ${readyLangs.size} of ${conf.languages.size} are ready")
        
        val r = new Random(197912)
        val annotated = r.shuffle(trainingSet.filter(t => !t.isLocation.isEmpty))
        val tweets = annotated.filter(t => !t.tweetId.isEmpty)
        val refSize = if(tweets.size < 500) 200 else tweets.size
        val demonyms = annotated.filter(t => t.category == "Demonym").take(refSize / 4)
        val people   = annotated.filter(t => t.category == "Person").take(refSize / 4)
        val locations = annotated.filter(t => t.source == GeoTrainingSource.epitweetrModel && t.isLocation == Some(true)).take(refSize)
        val noLocations = annotated.filter(t => t.source == GeoTrainingSource.epitweetrModel && t.isLocation == Some(false)).take(refSize) 
        val rescaled = tweets ++ demonyms ++ people ++ locations ++ noLocations
        println(s"tweets ${tweets.size} ++ dem ${demonyms.size} ++ peop ${people.size} ++ loc ${locations.size} ++ noloc ${noLocations.size}")
        
        val results = GeoTraining.splitTrainEvaluate(annotations = rescaled, trainingRatio = 0.7).cache
        import s.implicits._
        //ret.groupByKey(_._1).reduceGroups((a, b) => (a._1, a._2, a._3, a._4, a._5.sum(b._5))).map(p => (p._2._1, p._2._5)).toDF("test", "metric").select(col("test"), col("metric.*")).show
        //!((!t.forcedLocationName.isEmpty || !t.forcedLocationCode.isEmpty) && t.isLocation == Some(false))
        val ret = results.toJSON.collect
        l.msg(s"Evaluation finished")
        
        l.msg(s"Training models with full data now")
        

        GeoTraining.trainModels(annotations = GeoTraining.toTaggedText(rescaled).flatMap(tt => tt.toBIO()))
        //Setting models to unchanged 
        
        conf.languages.get.foreach{ l => 
          st.isUnchanged(path = Some(l.vectorsPath), checkPath = Some(s"${l.vectorsPath}.txt"), updateStamp = true) 
        }
        ret
          .foreach{line =>
             Await.result(caller ? ByteString(
               s"${sep}${line.toString}\n", 
               ByteString.UTF_8
             ), Duration.Inf)
             if(!jsonnl) sep = ","
          }
        
        if(!jsonnl) { 
          Await.result(caller ?  ByteString("]", ByteString.UTF_8), Duration.Inf)
        }
        caller ! Done
      }.recover {
        case e: Exception => 
          caller ! ByteString(s"[Stream--error]: ${e.getCause} ${e.getMessage}: ${e.getStackTrace.mkString("\n")}", ByteString.UTF_8)
      }.onComplete { case  _ =>
      }
    case b => 
      Future(EpitweetrActor.Failure(s"Cannot understund $b of type ${b.getClass.getName} as message")).pipeTo(sender())
  }
}

 
object GeonamesActor {
  case class TrainingSetRequest(excludedLangs:Seq[String], locationSamples:Boolean, jsonnl:Boolean, caller:ActorRef)
  case class TrainLanguagesRequest(trainingSet:Seq[GeoTraining], jsonnl:Boolean, caller:ActorRef)
  case class GeolocateTextsRequest(
    toGeo:Seq[TextToGeo], 
    minScore:Option[Int], 
    forcedGeo:Option[Map[String, String]], 
    forcedGeoCodes:Option[Map[String, String]], 
    topics:Option[Set[String]], 
    jsonnl:Boolean, 
    caller:ActorRef
  )
  case class LanguagesTrained(msg:String)
}


