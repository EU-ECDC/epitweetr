package org.ecdc.twitter 

import demy.storage.{Storage, WriteMode, FSNode}
import demy.mllib.text.Word2VecApplier
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
import org.ecdc.epitweetr.geo.GeoTraining
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
   def getGeoLikehoodModel(retrainWith:Option[Seq[GeoTraining]], geonames:Geonames)(implicit spark:SparkSession, storage:Storage) = {
      if(storage.getNode(modelPath).exists
          && !areVectorsNew()
          && retrainWith.isEmpty
      ) {
        LinearSVCModel.load(modelPath)
      } else if(!retrainWith.isEmpty){
        println(retrainWith) 
        val model = 
          new LinearSVC()
            .setFeaturesCol("feature")
            .setLabelCol("label")
        val trainData = getGeoVectors(geonames).select("feature", "label").union(getNonGeoVectors(geonames).select("feature", "label").limit(10000)).cache
        val trained = model.fit(trainData)
        trainData.unpersist
        trained.write.overwrite().save(modelPath)
        //Setting update timestamp
        storage.isUnchanged(path = Some(this.vectorsPath), checkPath = Some(s"${this.vectorsPath}.stamp"), updateStamp = true)  
        trained
      } else {
        throw new Exception("Cannot get a likelyhood model without training data and unprocessed vectors")
      }
   }
 }

object Language {
  val simpleSplitter = "\\s+"
  implicit class LangTools(val ds: Dataset[_]) {
    def addLikehoods(languages:Seq[Language], geonames:Geonames, vectorsColNames:Seq[String], langCodeColNames:Seq[String], likehoodColNames:Seq[String])(implicit spark:SparkSession, storage:Storage) = {
      Language.addLikehoods(
        df = ds.toDF
        , languages = languages
        , geonames = geonames
        , vectorsColNames = vectorsColNames
        , langCodeColNames = langCodeColNames
        , likehoodColNames = likehoodColNames
      ) 
    }
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
  def addLikehoods(
    df:DataFrame
    , languages:Seq[Language]
    , geonames:Geonames
    , vectorsColNames:Seq[String]
    , langCodeColNames:Seq[String]
    , likehoodColNames:Seq[String]
    )(implicit storage:Storage) = {
    implicit val spark = df.sparkSession
    val trainedModels = spark.sparkContext.broadcast(languages.map(l => (l.code -> l.getGeoLikehoodModel(retrainWith = None, geonames = geonames))).toMap)
    Some(
      df.rdd.mapPartitions{iter => 
        val rawPredict = 
          trainedModels.value.mapValues{model => 
            val method = model.getClass.getDeclaredMethod("predictRaw", classOf[MLVector])
            method.setAccessible(true) 
            (model, method)
          }

        iter.map{row =>
           Some(
             vectorsColNames.zip(langCodeColNames)
               .map{case (vectorsCol, langCodeCol) =>
                 Some((row.getAs[String](langCodeCol), row.getAs[Seq[Row]](vectorsCol).map(r => r.getAs[DenseVector]("vector"))))
                   .map{case(langCode, vectors) => 
                     rawPredict.get(langCode)
                       .map{case (model, method) =>
                         vectors.map{vector => 
                           if(vector == null)
                             //throw new Exception("Null vectors are not supported anymore")
                             0.4
                           else
                             Some(method.invoke(model, vector).asInstanceOf[DenseVector])
                               .map{scores =>
                                 val (yes, no) = (scores(1), scores(0))
                                 if(no>=0 && yes>=0) yes/(no + yes)
                                 else if(no>=0 && yes<=0) 0.5 - Math.atan(no-yes)/Math.PI
                                 else if(no<=0 && yes>=0) 0.5 + Math.atan(yes-no)/Math.PI
                                 else no/(no + yes)
                               }
                               .get
                         }
                       }
                       .getOrElse(null)
                   }
                   .get
               }
             )
             .map(scoreVectors => Row.fromSeq(row.toSeq ++ scoreVectors))
             .get
        }
      })
      .map(rdd => spark.createDataFrame(rdd, StructType(fields = df.schema.fields ++ likehoodColNames.map(colName => StructField(colName , ArrayType(DoubleType, true), true)))))
      .get
  }
  def array2Seq(array:Array[Language]) = array.toSeq
  def updateLanguages(trainingSet:Seq[GeoTraining], langs:Seq[Language], geonames:Geonames, indexPath:String, parallelism:Option[Int]=None) 
    (implicit spark:SparkSession, storage:Storage):Unit = {
      import spark.implicits._
      
      //Ensuring languages models are up to date
      langs.foreach(l => l.getGeoLikehoodModel(retrainWith = Some(trainingSet), geonames = geonames))
      val reuseExistingIndex = langs.map(l => !l.areVectorsNew()).reduce(_ && _)
      //Getting multilingual vectors
      val vectors = Language.multiLangVectors(langs)

      //Building vector index index if it is not  built 
      Seq(("Viva Chile constituyente, gracias Sébastian","es"), ("We did a very good job", "en")).toDF("text", "lang")
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
          , strategy="demy.mllib.index.PredictStrategy"
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
      ))
      .map(df => df.toDF(df.schema.map(f => 
          if(textLangCols.size == 1 && f.name == "array") s"${textLangCols.keys.head}_vec"
          else if(f.name.endsWith("_res")) f.name.replace("_res", "_vec") 
          else f.name
        ):_* ))
      .get
  }
} 
       


