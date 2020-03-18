package org.ecdc.twitter 

import demy.storage.{Storage, WriteMode, FSNode}
import org.apache.spark.sql.{SparkSession, Column, DataFrame, Row}
import org.apache.spark.sql.functions.{col, lit, udf}
import org.apache.spark.sql.types._
import org.apache.spark.ml.linalg.SQLDataTypes.VectorType 
import org.apache.spark.ml.linalg.{Vectors, DenseVector, Vector=>MLVector}
import org.apache.spark.ml.classification.{LinearSVC, LinearSVCModel}
import demy.mllib.text.Word2VecApplier
import java.lang.reflect.Method
import Geonames.GeoLookup 

case class Language(name:String, code:String, vectorsPath:String) {
   val modelPath = s"$vectorsPath.model"
   def getVectors()(implicit spark:SparkSession, storage:Storage) = { 
     import spark.implicits._
     spark.read.text(vectorsPath).as[String]
       .map(s => s.split(" "))
       .filter(a => a.size>2)
       .map(a => (a(0), Vectors.dense(a.drop(1).map(s => s.toDouble))))
   }

   def prepare(locationPath:String, dataPath:String):Unit = {
     
   }

   def getGeoLikehoodModel(geonames:Geonames)(implicit spark:SparkSession, storage:Storage) = {
      if(storage.getNode(modelPath).exists)
        LinearSVCModel.load(modelPath)
      else { 
        val langDico = this.getVectors().toDF("word", "vector") 
       
        val geoVectors = 
          langDico.geoLookup(col("word"), geonames, maxLevDistance= 0, minScore = 150)
            .where(col("geo_id").isNotNull)
            .select(lit(1.0).as("label"), col("vector").as("feature"))
            .limit(1000)
        val nonGeoVectors = 
          langDico.geoLookup(col("word"), geonames, maxLevDistance= 0, minScore = 30)
            .where(col("geo_id").isNull)
            .select(lit(0.0).as("label"), col("vector").as("feature"))
            .limit(10000)

        val model = 
          new LinearSVC()
            .setFeaturesCol("feature")
            .setLabelCol("label")
        val trainData = geoVectors.union(nonGeoVectors).cache
        val trained = model.fit(trainData)
        trainData.unpersist
        trained.save(modelPath)
        trained
      }
   }
       
   def addLikeHood(df:DataFrame, geonames:Geonames, vectorsColName:String, likehoodColName:String)(implicit spark:SparkSession, storage:Storage) = {
     val trained = spark.sparkContext.broadcast(this.getGeoLikehoodModel(geonames))
     Some(
       df.rdd.mapPartitions{iter => 
         val rawPredict =  { 
           val p = trained.value.getClass.getDeclaredMethod("predictRaw", classOf[MLVector])
           p.setAccessible(true) 
           p
         }
         iter.map{row => 
             Some(row.getAs[Array[DenseVector]](vectorsColName))
               .map(vectors => vectors
                 .map{vector => 
                   val scores = rawPredict.invoke(trained.value, vector).asInstanceOf[DenseVector]
                   val (yes, no) = (scores(1), scores(0))

                   if(no>=0 && yes>=0) yes/(no + yes)
                   else if(no>=0 && yes<=0) 0.5 - Math.atan(no-yes)/Math.PI
                   else if(no<=0 && yes>=0) 0.5 + Math.atan(yes-no)/Math.PI
                   else no/(no + yes)
                 }
               )
             .map(scoreVector => Row.fromSeq(row.toSeq :+ scoreVector))
             .get
         }
       })
       .map(rdd => spark.createDataFrame(rdd, df.schema.add(likehoodColName, ArrayType(VectorType, true))))
       .get
   }
} 
       


/*predictions.withColumn(probaColumnName, udf((rawPreds:Seq[MLVector]) =>{
                        val scores = rawPreds.map(rawPred => {
                        var preds = rawPred.toArray
                        if(preds.size!=2) throw new Exception("Score can only be calculated for binary class")
                        val Array(no, yes) = preds

                        // Return Platt's score if A > 0, otherwise approximation
                        if (A > 1e-20)
                            1.0 / (1 + exp(A * no + B))
                        else {
                            if(no>=0 && yes>=0) yes/(no + yes)
                            else if(no>=0 && yes<=0) 0.5 - Math.atan(no-yes)/Math.PI
                            else if(no<=0 && yes>=0) 0.5 + Math.atan(yes-no)/Math.PI
                            else no/(no + yes)
                        }
                    })
                    scores.toArray
//        1
        } ).apply(array(col(rawScoresColumnName))))
*/

      
/*     val vectoriser = word2VecApplier  =
      new Word2VecApplier()
       .setInputCol("")
       .setOutputCol("docVector")
       .setFormat("text")
       .setVectorsPath(vectors)
       .setIndexPath(s"$dataFolder")
       .setReuseIndexFile(true)
       .setAccentSensitive(false)
       .setCaseSensitive(true)
       .setLogMetrics(false)
       .setSumWords(false)


  def vectorIndex(spark:SparkSession)  =
      word2VecApplier
        .setSumWords(true)
        .toIndex(spark)


  def inputDF(spark:SparkSession) = {
    (
      this.readFormat match {
        case "csv" =>
          spark.read
            .option("sep", this.csvSeparator)
            .option("header", "true")
            .option("quote", this.csvQuote)
            .option("escape", this.csvEscape)
            .option("charset", this.csvCharset)
            .option("multiLine", this.csvMultiLine)
            .csv(feedbackPath)
        case "pubmed" =>
          pubmed.PubmedDir(feedbackPath).getFlattenDS(spark = spark)
        case "parquet" =>
          spark.read.parquet(feedbackPath)
        case _ => throw new Exception(s"unvalid format $readFormat has been provided")
      }
    ).select((
      Seq(concat(
        this.readColumns.map(c =>
            concat(
              coalesce(col(c), lit(""))
              , if(addColumnNameOnText)
                  when(
                    col(c).isNull || trim(col(c)) === lit("")
                    ,lit(" ")
                  ).otherwise(lit(s" $c "))
                else
                  lit(" ")
            )):_*).as("text")
      ) ++ (validationTagsCol match {
        case Some(valCol) => Seq(split(col(valCol), validationTagsSep).as(valCol))
        case _ => Seq[Column]()
      }) ++ (idColumn match {
        case Some(id) => Seq(col(id).as("id"))
        case None => Seq(lit(None.asInstanceOf[Option[String]]).as("id"))
      })
    ) :_*
    ).where(trim(col("text"))=!="")
  }*/
