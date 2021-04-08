package org.ecdc.epitweetr.fs

import akka.actor.{ActorSystem, Actor, Props}
import akka.stream.ActorMaterializer
import akka.pattern.{ask, pipe}
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.{HttpEntity, ContentType, ContentTypes, HttpResponse}
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.model.StatusCodes.InternalServerError
import akka.http.scaladsl.marshalling.ToResponseMarshallable
import akka.util.{Timeout}
import spray.json.{JsValue}
import scala.concurrent.duration._
import scala.util.{Try, Success, Failure}


object API {
  def run(port:Int) {

    import EpiSerialisation._
    implicit val actorSystem = ActorSystem("epitweetr")
    implicit val timeout: Timeout = 30.seconds //For ask property
    implicit val executionContext = actorSystem.dispatcher
    val luceneRunner = actorSystem.actorOf(Props(classOf[LuceneActor]))


    val route =
      extractUri { uri =>
        path("tweets") { // checks if path/url starts with model
          post {
            entity(as[JsValue]) { json  =>
              completeTry(Try{
                val tweets = tweetsV1Format.read(json)
                (luceneRunner ? tweets)
                  .map(o => o.asInstanceOf[LuceneActor.Success])
                  /*.map{
                    case s:LuceneActor.Success => s
                    case f:LuceneActor.Failure => f
                    case o => throw new Exception(f"Cannot interpret $o as a message")
                  }*/
              }, uri.toString())
            } ~ 
              entity(as[String]) { json  => 
              complete(LuceneActor.Failure(s"$json is not json :-P")) 
            } ~ {
              complete(LuceneActor.Failure(s"Cannot parse tweets")) 
            }
          }
        } ~ {
          complete(LuceneActor.Failure(s"Cannot find a route for uri $uri")) 
        }
    }


    Http().newServerAt("localhost", 8080).bind(route)
  }

  def completeTry(tryed:Try[ToResponseMarshallable], uri:String) = {
    tryed match {
      case Success(entity) =>
        complete(entity)
      case Failure(e) =>
        complete(defaultException(e, uri))
    }
  }
  def defaultException(e:Throwable, uri:String) = {
    val message = s"Request to $uri could not be handled normally" + "\n" + e.toString() + "\n" + e.getStackTrace.mkString("\n")
    println(message)
    HttpResponse(InternalServerError, entity = message)
  }

}
