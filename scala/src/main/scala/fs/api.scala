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
import org.ecdc.epitweetr.Settings
import akka.actor.ActorRef
import akka.Done
import akka.actor.ActorRef
import akka.stream.OverflowStrategy
import akka.stream.CompletionStrategy
import akka.stream.scaladsl._
import akka.http.scaladsl.model.HttpEntity.{Chunked, Strict}
import akka.util.ByteString
import java.time.Instant

object API {
  var actorSystemPointer:Option[ActorSystem] = None
  var oLuceneRunner:Option[ActorRef] = None
  def run(port:Int, epiHome:String) {
    import EpiSerialisation._
    implicit val actorSystem = ActorSystem("epitweetr")
    actorSystemPointer = Some(actorSystem)
    implicit val executionContext = actorSystem.dispatcher
    val conf = Settings(epiHome)
    val luceneRunner = actorSystem.actorOf(Props(classOf[LuceneActor], conf))
    oLuceneRunner = Some(luceneRunner)

    val route =
      extractUri { uri =>
        path("tweets") { // checks if path/url starts with model
          get {
            parameters("q".?, 
              "topic",
              "from"?(Instant.now.toString().take(10)), 
              "to"?(Instant.now.toString().take(10)), 
              "jsonnl".as[Boolean]?false, 
              "max".as[Int]?100
            ) { (query, topic, fromStr, toStr, jsonnl, max) => 
              implicit val timeout: Timeout = 30.seconds //For ask property
              val mask = "YYYY-MM-DDT00:00:00.000Z"
              val from =  Instant.parse(s"$fromStr${mask.substring(fromStr.size)}")
              val to =  Instant.parse(s"$toStr${mask.substring(toStr.size)}")
              val source: Source[akka.util.ByteString, ActorRef] = Source.actorRefWithBackpressure(
                ackMessage = ByteString("ok")
                , completionMatcher = {case Done => CompletionStrategy.immediately}
                , failureMatcher = PartialFunction.empty
              )
              val (actorRef, matSource) = source.preMaterialize()
              luceneRunner ! LuceneActor.SearchRequest(query, topic, from, to, max, jsonnl, actorRef) 
              complete(Chunked.fromData(ContentTypes.`application/json`, matSource.map{m =>
                if(m.startsWith(ByteString(s"[Stream--error]:"))) 
                  throw new Exception(m.decodeString("UTF-8"))
                else 
                  m
              }))
            }
          } ~
          post {
            withRequestTimeout(3000.seconds) {
              parameters("topic", "geolocate".as[Boolean]?true) { (topic, geolocate) =>
                entity(as[JsValue]) { json  =>
                  implicit val timeout: Timeout = 3000.seconds //For ask property
                  Try(tweetsV1Format.read(json)) match {
                    case Success(tweets) =>
                       val fut = (luceneRunner ? TopicTweetsV1(topic, tweets))
                         .map{
                            case LuceneActor.DatesProcessed(m, dates) => 
                              if(geolocate) luceneRunner ! LuceneActor.GeolocateRequest(TopicTweetsV1(topic, tweets))
                              (StatusCodes.OK, LuceneActor.DatesProcessed(m, dates))
                            case LuceneActor.Failure(m) => (StatusCodes.NotAcceptable, LuceneActor.DatesProcessed(m))
                            case o => (StatusCodes.InternalServerError, LuceneActor.DatesProcessed(s"Cannot interpret $o as a message"))
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
          }
        } ~ path("aggregate") { // checks if path/url starts with model
          post {
            entity(as[String]) { sourceExpressions  =>
              withRequestTimeout(600.seconds) {
                parameters("q", "jsonnl".as[Boolean]?false, "from", "to", "columns".as[String].*, "groupBy".as[String].*, "filterBy".as[String].*, "sortBy".as[String].*) { 
                (q, jsonnl, fromStr, toStr, columns, groupBy, filterBy, sortBy) =>
                   
                  val mask = "YYYY-MM-DDT00:00:00.000Z"
                  val from =  Instant.parse(s"$fromStr${mask.substring(fromStr.size)}")
                  val to =  Instant.parse(s"$toStr${mask.substring(toStr.size)}")
                  implicit val timeout: Timeout = 600.seconds //For ask property
                  val source: Source[akka.util.ByteString, ActorRef] = Source.actorRefWithBackpressure(
                    ackMessage = ByteString("ok")
                    , completionMatcher = {case Done => CompletionStrategy.immediately}
                    , failureMatcher = PartialFunction.empty
                  ).backpressureTimeout(FiniteDuration(600, "seconds"))
                  .idleTimeout(FiniteDuration(600, "seconds"))
                  val (actorRef, matSource) = source.preMaterialize()
                  luceneRunner ! LuceneActor.AggregateRequest(
                    q, 
                    from, 
                    to, 
                    columns.toSeq, 
                    groupBy.toSeq, 
                    filterBy.toSeq, 
                    sortBy.toSeq,
                    Seq(sourceExpressions)
                      .toSeq 
                      .map(tLine => 
                        Some(
                          tLine
                            .split("\\|\\|")
                            .toSeq
                            .map(_.trim)
                            .filter(_.size > 0)
                            //TODO: add file support.map(v => qParams.map(qPars => qPars.foldLeft(v)((curr, iter) => curr.replace(iter._1, iter._2))).getOrElse(v))
                          )
                          .map(s => (s(0), s.drop(1)))
                          .get
                       )
                      .toMap,
                    jsonnl, 
                    actorRef
                  ) 
                  complete(Chunked.fromData(ContentTypes.`application/json`, matSource.map{m =>
                    if(m.startsWith(ByteString(s"[Stream--error]:"))) 
                      throw new Exception(m.decodeString("UTF-8"))
                    else 
                      m
                  }))
                }
              }
            }
          }
        } ~ path("geolocated") { // checks if path/url starts with model
          post {
            parameters("created".as[String].*) { createdStr => 
              val mask = "YYYY-MM-DDT00:00:00.000Z"
              val dates =  createdStr.map(d => Instant.parse(s"$d${mask.substring(d.size)}"))
              if(createdStr.size > 0) {
                entity(as[JsValue]) { json  =>
                  implicit val timeout: Timeout = 3000.seconds //For ask property
                  Try(geolocatedsFormat.read(json)) match {
                    case Success(geolocateds) =>
                       val fut = (luceneRunner ? LuceneActor.GeolocatedsCreated(geolocateds, dates.toSeq))
                         .map{
                            case LuceneActor.Success(m) => (StatusCodes.OK, LuceneActor.Success(m))
                            case LuceneActor.Failure(m) => (StatusCodes.NotAcceptable, LuceneActor.Success(m))
                            case o => (StatusCodes.InternalServerError, LuceneActor.Success(s"Cannot interpret $o as a message"))
                          }
                       complete(fut) 
                     case Failure(e) =>
                       println(s"Cannot interpret the provided body as geolocated array:\n $e, ${e.getStackTrace.mkString("\n")}") 
                       complete(StatusCodes.NotAcceptable, LuceneActor.Failure(s"Cannot interpret the provided body as a gelocated array:\n $e")) 
                  } 
                } ~ entity(as[String]) { value  => 
                  logThis(value)
                  complete(StatusCodes.NotAcceptable, LuceneActor.Failure(s"This endpoint expects json, got this instead: \n$value")) 
                }
              } else {
                complete(StatusCodes.NotAcceptable, LuceneActor.Failure(s"Missing parameter created with reference dates of geolocated tweets")) 
              }
            }
          }
        } ~ path("commit") { // commit the tweets sent
          post {
            withRequestTimeout(600.seconds) {
            implicit val timeout: Timeout = 600.seconds //For ask property
            val fut =  (luceneRunner ? LuceneActor.CommitRequest())
              .map{
                 case LuceneActor.Success(m) => (StatusCodes.OK, LuceneActor.Success(m))
                 case LuceneActor.Failure(m) => (StatusCodes.NotAcceptable, LuceneActor.Success(m))
                 case o => (StatusCodes.InternalServerError, LuceneActor.Success(s"Cannot interpret $o as a message"))
               }
             complete(fut)
            }
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
    oLuceneRunner.map{lr =>
      implicit val timeout: Timeout = 30.seconds //For ask property
      implicit val executionContext = actorSystemPointer.get.dispatcher
      (lr ? LuceneActor.CloseRequest)
        .onComplete{_ => 
          actorSystemPointer.map(as => as.terminate)
          actorSystemPointer = None
          oLuceneRunner = None
      }
    }
  }
}
