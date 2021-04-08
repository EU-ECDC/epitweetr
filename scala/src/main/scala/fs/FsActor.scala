package org.ecdc.epitweetr.fs

import akka.pattern.{ask, pipe}
import scala.concurrent.Future
import akka.actor.{ActorSystem, Actor, ActorLogging, Props}


class LuceneActor() extends Actor with ActorLogging {
  implicit val executionContext = context.system.dispatchers.lookup("lucene-dispatcher")
  def receive = {
    case t:TweetsV1 =>
      Future(t.items.size)
        .map{c =>
          LuceneActor.Success(s"$c tweets properly processed")
        }.pipeTo(sender())
    case b => 
      Future(LuceneActor.Failure(s"Cannot understund $b as message")).pipeTo(sender())
  }
}


object LuceneActor {
  case class Success(msg:String)
  case class Failure(msg:String)
}
