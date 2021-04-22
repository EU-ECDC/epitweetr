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
import org.ecdc.twitter.schemas
import scala.collection.JavaConverters._

class LuceneActor() extends Actor with ActorLogging {
  implicit val executionContext = context.system.dispatchers.lookup("lucene-dispatcher")
  implicit val timeout: Timeout = 30.seconds //For ask property
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
    case LuceneActor.SearchRequest(query, jsonnl, caller) => 
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
    case LuceneActor.AggregateRequest(query, jsonnl, caller) =>
      val spark = LuceneActor.getSparkSession
      val sc = spark.sparkContext
      Future {
        val index = LuceneActor.getIndex
        if(!jsonnl) { 
          Await.result(caller ?  ByteString("[", ByteString.UTF_8), Duration.Inf)
        }
        var first = true
        val rdd = sc.parallelize(
          index.searchAll(query)
            .map(doc => {println("1");EpiSerialisation.luceneDocFormat.write(doc).toString})
            .toSeq
        ).map(s => {println("B");s})

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
}


object LuceneActor {
  case class Success(msg:String)
  case class Failure(msg:String)
  case class CommitRequest()
  case class SearchRequest(query:String, jsonnl:Boolean, caller:ActorRef)
  case class AggregateRequest(query:String, jsonnl:Boolean, caller:ActorRef
    /*, from:LocalDateTime
    , to:LocalDateTime
    , columns:Seq[String]
    , groupBy:Seq[String]
    , filterBy:Seq[String]
    , sortBy:Seq[String]
    , sourceExpressions:Map[String, Seq[String]]
    , langs:Seq[Language]
    , parallelism:Option[Int]
    , jsonnl:Boolean
  */)
  val lock = "lock"
  var _index:Option[TweetIndex]=None
  var _spark:Option[SparkSession] = None
  def commit() = {
    close(closeDirectory = true)
  }
  def close(closeDirectory:Boolean= true) = {
    lock.synchronized {
      _index match{
        case Some(i) =>
          _index = None
          val now = System.nanoTime 
          if(i.writer.isOpen) {
            i.writer.commit()
            i.writer.close()
          }
          if(closeDirectory) {
            i.index.close()
          }
          println(s"commit done on ${(System.nanoTime - now) / 1000000000} secs")
        case _ =>
      }
      _spark match{
        case Some(spark) =>
          spark.stop()
        case _ =>
      }
    }
  }
  def getIndex = {
    lock.synchronized {
      if(_index.isEmpty || !_index.get.writer.isOpen) {
        _index = Some(TweetIndex("Z:/epitweetr/tweets/fs"))
      }
    }
    _index.get
  }
  def getSparkSession = {
    lock.synchronized {
      if(_spark.isEmpty) {
        _spark =  Some(JavaBridge.getSparkSession()) 
      }
    }
    _spark.get
  }
}
