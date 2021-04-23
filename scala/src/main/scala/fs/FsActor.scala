package org.ecdc.epitweetr.fs

import org.ecdc.twitter.JavaBridge
import akka.pattern.{ask, pipe}
import scala.concurrent.{Future, Await}
import scala.concurrent.duration._
import akka.actor.{ActorSystem, Actor, ActorLogging, Props}
import akka.pattern.ask
import akka.stream.scaladsl.{Source}
import akka.actor.ActorRef
import akka.Done
import akka.util.ByteString
import akka.util.{Timeout}
import org.apache.spark.sql.{SparkSession, Column, Dataset, Row, DataFrame}
import java.time.LocalDateTime
import org.ecdc.twitter.{Settings, schemas}
import scala.collection.JavaConverters._
import scala.collection.mutable.HashMap
import java.time.temporal.{IsoFields, ChronoUnit}
import java.time.{Instant, ZoneOffset, LocalDateTime}
import java.nio.file.Paths
class LuceneActor(conf:Settings) extends Actor with ActorLogging {
  implicit val executionContext = context.system.dispatchers.lookup("lucene-dispatcher")
  implicit val timeout: Timeout = 30.seconds //For ask property
  implicit val cnf = conf
  def receive = {
    case TopicTweetsV1(topic, ts) =>
      Future{
        val index = LuceneActor.getIndex
        ts.items.foreach(t => index.indexTweet(t, topic))
        ts.items.size
      }
      .map{c =>
        LuceneActor.Success(s"$c tweets properly processed")
      }
      .pipeTo(sender())
    case Geolocateds(items) =>
      Future{
        val index = LuceneActor.getIndex
        items.foreach(g => index.indexGeolocated(g))
        items.size
      }
      .map{c =>
        LuceneActor.Success(s"$c geolocated properly processed")
      }
      .pipeTo(sender())
    case ts:LuceneActor.CommitRequest =>
      Future{
        val index = LuceneActor.getIndex
        LuceneActor.commit()
      }
      .map{c =>
        LuceneActor.Success(s"$c Commit done processed")
      }
      .pipeTo(sender())
    case LuceneActor.SearchRequest(query, from, to, jsonnl, caller) => 
      Future {
        val index = LuceneActor.getIndex
        if(!jsonnl) { 
          Await.result(caller ?  ByteString("[", ByteString.UTF_8), Duration.Inf)
        }
        var first = true
        index.searchAll(query)
          .map(doc => EpiSerialisation.luceneDocFormat.write(doc))
          .foreach{line => 
            Await.result(caller ? ByteString(
              s"${if(!jsonnl && !first) "\n," else if(jsonnl && !first) "\n" else "" }${line.toString}", 
              ByteString.UTF_8
            ), Duration.Inf)
            first = false
          }
        if(!jsonnl) { 
          Await.result(caller ?  ByteString("]", ByteString.UTF_8), Duration.Inf)
        }
        caller ! Done
      }.recover {
        case e: Exception => 
          caller ! ByteString(s"[Stream--error]: ${e.getMessage}: ${e.getStackTrace.mkString("\n")}", ByteString.UTF_8)
      }
    case LuceneActor.AggregateRequest(query, from, to, columns, groupBy, filterBy, sortBy, sourceExpressions, jsonnl, caller) =>
      val spark = LuceneActor.getSparkSession()
      val sc = spark.sparkContext
      Future {
        val keys = LuceneActor.getReadKeys(from, to)
        var first = true
        val rdd = sc.parallelize(keys)
          .repartition(conf.sparkCores)
          .mapPartitions{iter => 
            var lastKey = ""
            var index:TweetIndex=null 
            iter.flatMap{key => 
              if(lastKey != key)
                index = LuceneActor.getIndex(key)
              index.searchAll(query)
                .map(doc => {println("1");EpiSerialisation.luceneDocFormat.write(doc).toString})
            }
          }.map(s => {println("B");s})

        val df = spark.read.schema(schemas.geoLocatedTweetSchema)
          .json(rdd)
          .groupBy("created_date")
          .count()
          .toJSON
          .toLocalIterator
          .asScala
          .foreach{line => 
            Await.result(caller ? ByteString(
              s"${if(!jsonnl && !first) "\n," else if(jsonnl && !first) "\n" else "" }${line}", 
              ByteString.UTF_8
            ), Duration.Inf)
            first = false
          }
        if(!jsonnl) { 
          Await.result(caller ?  ByteString("[", ByteString.UTF_8), Duration.Inf)
        }
        if(!jsonnl) { 
          Await.result(caller ?  ByteString("]", ByteString.UTF_8), Duration.Inf)
        }
        caller ! Done
      }.recover {
        case e: Exception => 
          caller ! ByteString(s"[Stream--error]: ${e.getMessage}: ${e.getStackTrace.mkString("\n")}", ByteString.UTF_8)
      }
    case b => 
      Future(LuceneActor.Failure(s"Cannot understund $b of type ${b.getClass.getName} as message")).pipeTo(sender())
  }
  def
}


object LuceneActor {
  case class Success(msg:String)
  case class Failure(msg:String)
  case class CommitRequest()
  case class SearchRequest(query:String, from:Instant, to:Instant, jsonnl:Boolean, caller:ActorRef)
  case class AggregateRequest(
    query:String, 
    from:Instant, 
    to:Instant, 
    columns:Seq[String], 
    groupBy:Seq[String], 
    filterBy:Seq[String], 
    sortBy:Seq[String], 
    sourceExpressions:Map[String, Seq[String]], 
    jsonnl:Boolean, 
    caller:ActorRef 
  )
  val _dirs:HashMap[String, String] = HashMap[String, String]()
  val _indexes:HashMap[String, TweetIndex] = HashMap[String, TweetIndex]()
  var _spark:Option[SparkSession] = None
  def commit() = {
    close(closeDirectory = true)
  }
  def close(closeDirectory:Boolean= true) = {
    _dirs.synchronized {
      var now:Option[Long] = None 
      _dirs.foreach{ case (key, path) =>
          now = now.orElse(Some(System.nanoTime))
          if(i.writer.isOpen) {
            i.writer.commit()
            i.writer.close()
          }
          if(closeDirectory) {
            i.index.close()
          }
        case _ =>
      }
      _indexes.clear
      now.map(n => println(s"commit done on ${(System.nanoTime - n) / 1000000000} secs"))
      _spark match{
        case Some(spark) =>
          spark.stop()
        case _ =>
      }
    }
  }
  /*def getIndex(forInstant:Instant)(implicit conf:Settings):TweetIndex = {
    val utc = LocalDateTime.ofInstant(forInstant, ZoneOffset.UTC)
    val key = Some(s"${utc.get(IsoFields.WEEK_BASED_YEAR)*100 + utc.get(IsoFields.WEEK_OF_WEEK_BASED_YEAR)}").map(s => s"${s.take(4)}.${s.substring(4)}")
    getIndex(key)
  }*/
  def getIndex(key:String)(implicit conf:Settings):TweetIndex = {
    _dirs.synchronized {
      if(!_dirs.contains(key)) {
        conf.open()
        _dirs(key) = Paths.get(conf.epiHomie, "fs", key).toString()
      }
    }
    _dirs(key).synchronized {
      if(_indexes.get(key).isEmpty || !_indexes(key).writer.isOpen) {
        _indexes(key) = TweetIndex(_dirs(key))
      }
    }
    _indexes(key)
  }

  def getSparkSession()(implicit conf:Settings) = {
    _dirs.synchronized {
      conf.open()
      if(_spark.isEmpty) {
        _spark =  Some(JavaBridge.getSparkSession(conf.sparkCores.gesOrEsle(0))) 
      }
    }
    _spark.get
  }
  def getReadKeys(from:Instant, to:Instant, conf:Settings) = 
     Range(0, ChronoUnit.DAYS.between(from, to).toInt)
       .map(i => from.plus(i,  ChronoUnit.DAYS))
       .map(i => LocalDateTime.ofInstant(i, ZoneOffset.UTC))
       .map(utc => s"${utc.get(IsoFields.WEEK_BASED_YEAR)*100 + utc.get(IsoFields.WEEK_OF_WEEK_BASED_YEAR)}")
       .distinct
       .map(s => s"${s.take(4)}.${s.substring(4)}")
       .filter(key => Paths.get(conf.epiHome, "fs", key).exists())
