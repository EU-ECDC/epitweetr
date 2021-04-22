package org.ecdc.epitweetr.fs

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
      
    case b => 
      Future(LuceneActor.Failure(s"Cannot understund $b of type ${b.getClass.getName} as message")).pipeTo(sender())
  }
}


object LuceneActor {
  case class Success(msg:String)
  case class Failure(msg:String)
  case class CommitRequest()
  case class SearchRequest(query:String, jsonnl:Boolean, caller:ActorRef)
  val lock = "lock"
  var _index:Option[TweetIndex]=None

  def commit() = {
    close()
  }
  def close() = {
    lock.synchronized {
      _index.foreach{i =>
        print("commiting")
        i.writer.commit()
        i.writer.close()
        i.index.close()
      } 
    }
    _index.get
  }
  def getIndex = {
    lock.synchronized {
      if(_index.isEmpty || !_index.get.writer.isOpen) {
        _index = Some(TweetIndex("Z:/epitweetr/tweets/fs"))
      }
    }
    _index.get
  }
}
