package org.ecdc.twitter 

import org.ecdc.epitweetr.geo.{Geonames}
import demy.storage.{Storage, WriteMode, FSNode}
import demy.mllib.index.implicits._
import demy.mllib.linalg.implicits._
import demy.util.{log => l, util}
import org.apache.spark.sql.{SparkSession, Column, DataFrame, Row, Dataset}
import org.apache.spark.sql.functions.{col, lit, udf, concat}
import org.apache.spark.sql.types._
import org.apache.spark.ml.linalg.SQLDataTypes.VectorType 
import org.apache.spark.ml.linalg.{Vectors, DenseVector, Vector=>MLVector}
import scala.collection.mutable.HashMap
import org.apache.spark.ml.classification.{LinearSVC, LinearSVCModel}
import java.lang.reflect.Method
import Geonames.Geolocate

case class Language(name:String, code:String, vectorsPath:String) {
   val modelPath = s"$vectorsPath.model"
   def getVectors()(implicit spark:SparkSession, storage:Storage) = { 
     import spark.implicits._
     spark.read.text(vectorsPath).as[String]
       .map(s => s.split(" "))
       .filter(a => a.size>2)
       .map(a => (a(0), Vectors.dense(a.drop(1).map(s => s.toDouble)), this.code))
   }
   def getVectorsSize()(implicit spark:SparkSession, storage:Storage) = { 
     import spark.implicits._
     spark.read.text(vectorsPath).as[String]
       .map(s => s.split(" "))
       .filter(a => a.size>2)
       .map(a => a.drop(1).size)
       .head
   }
   def getUnknownVector(n:Int) = Vectors.dense(Array.range(0, n).map(s => 1.0))

   def getVectorsDF()(implicit spark:SparkSession, storage:Storage) = {
     this.getVectors().toDF("word", "vector", "lang")
   }

   def getStopWords(implicit spark:SparkSession, storage:Storage) = {
     import spark.implicits._
     this.getVectors().map(_._1).take(200).toSet
   }

   def getGeoVectors(geonames:Geonames)(implicit spark:SparkSession, storage:Storage) = {
     import spark.implicits._
     val topVectors  = spark.sparkContext.broadcast(this.getVectorsDF().select("word", "vector").as[(String, MLVector)].take(100000).toMap)

     val geoVec = (
       geonames
         .getLocationSample()
         .map(loc => (loc, loc.split(" ").map(t => topVectors.value.get(t))))
         .filter{t => t match{ case (loc, vectors) => vectors.forall(!_.isEmpty)}}
         .map{case(loc, vectors) => (loc, vectors.flatMap(s => s).reduce((v1, v2) => v1.sum(v2)))}
         .toDF("location", "vector")
     ) 
     geoVec 
       .select(col("location"), lit(1.0).as("label"), col("vector").as("feature"))
   }
   def getNonGeoVectors(geonames:Geonames)(implicit spark:SparkSession, storage:Storage) = {
     val langDico = this.getVectorsDF()
     langDico.geolocate(Map("word"->None), geonames = geonames, maxLevDistance= 0, minScore = 3)
       .where(col("geo_id").isNull)
       .select(col("word"), col("_score_"), lit(0.0).as("label"), col("vector").as("feature"))
   }

   def areVectorsNew()(implicit storage:Storage) = !storage.isUnchanged(path = Some(this.vectorsPath), checkPath = Some(s"${this.vectorsPath}.stamp"), updateStamp = false) 

 }

object Language {
  val simpleSplitter = "\\s+"
  implicit class LangTools(val ds: Dataset[_]) {
    def vectorize(languages:Seq[Language] , reuseIndex:Boolean = true, indexPath:String, textLangCols:Map[String, Option[String]], tokenizerRegex: String = simpleSplitter
    )(implicit storage:Storage) = {
      Language.vectorizeTextDS(
        ds = ds
        , langs = languages
        , reuseIndex = reuseIndex
        , indexPath = indexPath
        , textLangCols = textLangCols
        , tokenizerRegex = tokenizerRegex
      )
    }
  }
  def array2Seq(array:Array[Language]) = array.toSeq
  def updateLanguageIndexes(langs:Seq[Language], geonames:Geonames, indexPath:String) 
    (implicit spark:SparkSession, storage:Storage) =  {
      import spark.implicits._
      val reuseExistingIndex = langs.map(l => !l.areVectorsNew()).reduce(_ && _)
      //Getting multilingual vectors
      if(reuseExistingIndex)
        l.msg("Reusing existing index for vectors")
      else
        l.msg("Change in vectors detected, recreating the index")
      val vectors = Language.multiLangVectors(langs)
      //Building vector index index if it is not  built 
      Seq(("Viva Chile constituyente","es"), ("We did a very good job", "en")).toDF("text", "lang")
        .luceneLookup(right = vectors
          , query = udf((text:String, lang:String) => text.split(" ").map(w => s"${w}LANG$lang")).apply(col("text"), col("lang")).as("tokens")
          , popularity = None
          , text = col("word")
          , rightSelect= Array(col("vector"))
          , leftSelect= Array(col("*"))
          , maxLevDistance = 0
          , indexPath = indexPath
          , reuseExistingIndex = reuseExistingIndex
          , indexPartitions = 1
          , maxRowsInMemory=1
          , indexScanParallelism = 1
          , tokenizeRegex = None
          , caseInsensitive = false
          , minScore = 0.0
          , boostAcronyms=false
          , strategy="demy.mllib.index.StandardStrategy"
        )
        .show
      
  }

  def multiLangVectors(langs:Seq[Language]) (implicit spark:SparkSession, storage:Storage) 
    = langs.map(l => l.getVectorsDF().select(concat(col("word"), lit("LANG"), col("lang")).as("word"), col("vector"))).reduce(_.union(_))

  def vectorizeText(
    text:Seq[String]
    , lang:Seq[String]
    , langs:Seq[Language]
    , reuseIndex:Boolean = true
    , indexPath:String
    , tokenizerRegex: String = simpleSplitter
  )(implicit spark:SparkSession, storage:Storage) = {
    import spark.implicits._
    vectorizeTextDS(text.zip(lang).toDF("text", "lang"), langs = langs, reuseIndex = reuseIndex, indexPath = indexPath, textLangCols = Map("text" ->Some("lang")), tokenizerRegex = tokenizerRegex)
  }

  def vectorizeTextDS(
    ds:Dataset[_]
    , langs:Seq[Language]
    , reuseIndex:Boolean = true
    , indexPath:String
    , textLangCols:Map[String, Option[String]] 
    , tokenizerRegex: String = simpleSplitter
  )(implicit storage:Storage) = {
    implicit val spark =  ds.sparkSession
    val unk = langs(0).getUnknownVector(langs(0).getVectorsSize())
    val vectors = Language.multiLangVectors(langs)
    Some(ds)
      .map(df => ds
        .luceneLookups(right = vectors
          , queries = textLangCols.toSeq.flatMap{
              case (textCol, Some(lang)) => 
                Some(
                  udf((text:String, lang:String) =>
                    if(text == null) 
                      Array[String]() 
                    else 
                      text.replaceAll(tokenizerRegex, " ")
                        .split(simpleSplitter)
                        .filter(_.size > 0)
                        .map(w => s"${w}LANG$lang")
                   ).apply(col(textCol), col(lang)).as(textCol)
                 )
              case _ => None
            }
          , popularity = None
          , text = col("word")
          , rightSelect= Array(col("vector"))
          , leftSelect= Array(col("*"))
          , maxLevDistance = 0
          , indexPath = indexPath
          , reuseExistingIndex = reuseIndex
          , indexPartitions = 1
          , maxRowsInMemory=1
          , indexScanParallelism = 1
          , tokenizeRegex = None
          , caseInsensitive = false
          , minScore = 0.0
          , boostAcronyms=false
          , strategy="demy.mllib.index.StandardStrategy"
          , defaultValue = Some(Row(unk)) 
      ))
      .map(df => df.toDF(df.schema.map(f => 
          if(textLangCols.size == 1 && f.name == "array") s"${textLangCols.keys.head}_vec"
          else if(f.name.endsWith("_res")) f.name.replace("_res", "_vec") 
          else f.name
        ):_* ))
      .get
  }
} 
       


