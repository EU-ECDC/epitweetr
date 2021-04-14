package org.ecdc.epitweetr.fs

import akka.pattern.{ask, pipe}
import scala.concurrent.Future
import akka.actor.{ActorSystem, Actor, ActorLogging, Props}
import akka.pattern.ask
import akka.stream.scaladsl.{Source}


class LuceneActor() extends Actor with ActorLogging {
  implicit val executionContext = context.system.dispatchers.lookup("lucene-dispatcher")
  def receive = {
    case TopicTweetsV1(topic, ts) =>
      Future{
        val index = LuceneActor.getIndex
        ts.items.map(t => index.indexTweet(t, topic))
        ts.items.size
      }
      .map{c =>
        LuceneActor.Success(s"$c tweets properly processed")
      }
      .pipeTo(sender())
    case ts:LuceneActor.CommitRequest =>
      Future{
        val index = LuceneActor.getIndex
        LuceneActor.getIndex.commit()
      }
      .map{c =>
        LuceneActor.Success(s"$c Commit done processed")
      }
      .pipeTo(sender())
    //case LuceneActor.SearchRequest(query) => 
    //  val index = LuceneActor.getIndex
    //  val it = 
    //  Source.fromIterator(() => index.searchAll(query)).pipeTo(sender())
    //  
    case b => 
      Future(LuceneActor.Failure(s"Cannot understund $b of type ${b.getClass.getName} as message")).pipeTo(sender())
  }
}


object LuceneActor {
  case class Success(msg:String)
  case class Failure(msg:String)
  case class CommitRequest()
  case class SearchRequest(query:String)
  val lock = "lock"
  var _index:Option[TweetIndex]=None

  def getIndex = {
    lock.synchronized {
      if(_index.isEmpty || !_index.get.writer.isOpen) {
        _index = Some(TweetIndex("Z:/epitweetr/tweets/fs"))
      }
    }
    _index.get
  }
}
