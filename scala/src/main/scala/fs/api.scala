package org.ecdc.epitweetr.fs

import akka.actor.{ActorSystem, Actor, Props}
import akka.stream.ActorMaterializer
import akka.pattern.{ask, pipe}
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.{HttpEntity, ContentType, ContentTypes, HttpResponse}
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.model.{StatusCodes, StatusCode}
import akka.http.scaladsl.marshalling.ToResponseMarshallable
import akka.util.{Timeout}
import spray.json.{JsValue}
import scala.concurrent.duration._
import scala.util.{Try, Success, Failure}

import akka.actor.ActorRef
import akka.Done
import akka.actor.ActorRef
import akka.stream.OverflowStrategy
import akka.stream.CompletionStrategy
import akka.stream.scaladsl._
import akka.http.scaladsl.model.HttpEntity.{Chunked, Strict}

object API {
  var actorSystemPointer:Option[ActorSystem] = None
  def run(port:Int) {

    import EpiSerialisation._
    implicit val actorSystem = ActorSystem("epitweetr")
    actorSystemPointer = Some(actorSystem)
    implicit val timeout: Timeout = 30.seconds //For ask property
    implicit val executionContext = actorSystem.dispatcher
    val luceneRunner = actorSystem.actorOf(Props(classOf[LuceneActor]))


    val route =
      extractUri { uri =>
        path("tweets") { // checks if path/url starts with model
          get {
            parameters("q", "jsonnl".as[Boolean]?false) { (q, jsonnl) => 
              val source: Source[akka.util.ByteString, ActorRef] = Source.actorRef(
                completionMatcher = {case Done => CompletionStrategy.immediately}
                , failureMatcher = PartialFunction.empty
                , bufferSize = 100
                , overflowStrategy = OverflowStrategy.fail
              )
              val (actorRef, matSource) = source.preMaterialize()
              luceneRunner ! LuceneActor.SearchRequest(q, jsonnl, actorRef) 
              complete(Chunked.fromData(ContentTypes.`application/json`, matSource))
            }
          } ~
          post {
            parameters("topic") { (topic) =>
              entity(as[JsValue]) { json  =>
                Try(tweetsV1Format.read(json)) match {
                  case Success(tweets) =>
                     val fut = (luceneRunner ? TopicTweetsV1(topic, tweets))
                       .map{
                          case LuceneActor.Success(m) => (StatusCodes.OK, LuceneActor.Success(m))
                          case LuceneActor.Failure(m) => (StatusCodes.NotAcceptable, LuceneActor.Success(m))
                          case o => (StatusCodes.InternalServerError, LuceneActor.Success(s"Cannot interpret $o as a message"))
                        }
                     complete(fut) 
                   case Failure(e) =>
                     println(s"Cannot interpret the provided body as a respose of the Tweet Search API v1.1:\n $e, ${e.getStackTrace.mkString("\n")}") 
                     complete(StatusCodes.NotAcceptable, LuceneActor.Failure(s"Cannot interpret the provided body as a respose of the Tweet Search API v1.1:\n $e")) 
                } 
              } ~ 
                entity(as[String]) { value  => 
                logThis(value)
                complete(StatusCodes.NotAcceptable, LuceneActor.Failure(s"This endpoint expects json, got this instead: \n$value")) 
              }
            } ~ {
              complete(StatusCodes.NotImplemented, LuceneActor.Failure(s"Missing expected parameter topic")) 
            }
          }
        } ~ path("commit") { // commit the tweets sent
          post {
            val fut =  (luceneRunner ? LuceneActor.CommitRequest())
              .map{
                 case LuceneActor.Success(m) => (StatusCodes.OK, LuceneActor.Success(m))
                 case LuceneActor.Failure(m) => (StatusCodes.NotAcceptable, LuceneActor.Success(m))
                 case o => (StatusCodes.InternalServerError, LuceneActor.Success(s"Cannot interpret $o as a message"))
               }
             complete(fut) 
          }
        } ~ {
          complete(LuceneActor.Failure(s"Cannot find a route for uri $uri")) 
        }
    }

    Http().newServerAt("localhost", 8080).bind(route)
  }
  def logThis(log:String) = {
    import java.nio.file.{Paths, Files}
    import java.nio.charset.StandardCharsets
    import java.nio.file.StandardOpenOption
    Files.write(Paths.get(s"${System.getProperty("user.home")}/akka-epi.json"), log.getBytes(StandardCharsets.UTF_8), StandardOpenOption.CREATE)
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
    HttpResponse(StatusCodes.InternalServerError, entity = message)
  }
  def stop() {
    println("Closing the index")
    LuceneActor.getIndex.close()
    actorSystemPointer.map(as => as.terminate)
    actorSystemPointer = None
  }
}