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
import org.ecdc.epitweetr.fs.API 
object Tweets {
  val twitterSplitter = "((http|https|HTTP|HTTPS|ftp|FTP)://(\\S)+|[^\\p{L}]|@+|#+|(?<!(^|[A-Z]))(?=[A-Z])|(?<!^)(?=[A-Z][a-z]))+|RT|via|vía"
  def main(args: Array[String]): Unit = {
    val cmd = Map(
     "fsService" -> Set("epiHome")
     , "updateGeonames" -> Set("geonamesSource", "geonamesDestination", "geonamesSimplify", "assemble", "index", "parallelism")
      , "updateLanguages" -> Set("langCodes", "langNames", "langPaths", "geonamesSource", "geonamesDestination", "geonamesSimplify", "langIndexPath", "parallelism")
    )
    if(args == null || args.size < 3 || !cmd.contains(args(0)) || args.size % 2 == 0 ) 
      l.msg(s"first argument must be within ${cmd.keys} and followed by a set of 'key' 'values' parameters, but the command ${args(0)} is followed by ${args.size -1} values")
    else {
      val command = args(0)
      val params = Seq.range(0, (args.size -1)/2).map(i => (args(i*2 + 1), args(i*2 + 2))).toMap
      if(!cmd(command).subsetOf(params.keySet))
        l.msg(s"Cannot run $command, expected named parameters are ${cmd(command)}")
      else if(command == "fsService") {
        API.run(params.get("epiHome").get)
      } else if(command == "updateGeonames") {
         implicit val spark = JavaBridge.getSparkSession(params.get("parallelism").map(_.toInt).getOrElse(0)) 
         implicit val storage = JavaBridge.getSparkStorage(spark)
         import spark.implicits._
         val geonames = Geonames(params.get("geonamesSource").get, params.get("geonamesDestination").get,  params.get("geonamesSimplify").get.toBoolean)
         if(params.get("assemble").map(s => s.toBoolean).getOrElse(false))
           geonames.getDataset(reuseExisting = false)
         if(params.get("index").map(s => s.toBoolean).getOrElse(false)) {
           geonames.geolocateText(text=Seq("Viva Chile"), reuseGeoIndex = false).show
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
           , geonames = Geonames( params.get("geonamesSource").get,  params.get("geonamesDestination").get,  params.get("geonamesSimplify").get.toBoolean)
           , indexPath = params.get("langIndexPath").get
           , parallelism = params.get("parallelism").map(_.toInt)
         )
      } else {
        throw new Exception("Not implemented @epi") 
      }
    } 
  }
}
  


