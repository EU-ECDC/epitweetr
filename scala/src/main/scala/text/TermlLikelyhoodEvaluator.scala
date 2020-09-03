package demy.mllib.text;

import org.apache.spark.ml.linalg.{Vector => MLVector}
import org.apache.spark.ml.classification.{Classifier, ClassificationModel}
import org.apache.spark.ml.linalg.{Vector => MLVector, Vectors, VectorUDT}
import org.apache.spark.ml.classification.{Classifier, ClassificationModel}
import org.apache.spark.ml.tuning.{ParamGridBuilder, TrainValidationSplit}
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.sql.{Column, Dataset, Row}
import org.apache.spark.ml.classification.{LinearSVC, LinearSVCModel}
import org.apache.spark.ml.attribute.AttributeGroup
import org.apache.spark.ml.param.shared._
import demy.mllib.params.{HasProbabilityCol, HasFeaturesCol}
import demy.mllib.index.implicits._
import demy.mllib.linalg.implicits._
import scala.math.{exp, log => Log}
import org.apache.spark.sql.functions.{row_number, split, lower, size, col, udf, rand, array, lit, expr}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.types._
import org.apache.spark.ml.param.{Params, Param, ParamMap}
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.ml.{Transformer, Estimator, PipelineStage}

/*class TermlLikelyhoodEvaluator {
  
   * Model Training:
   *   Vector Dictionary: vectors:DataFrame, WordColumnName:String, VectorColumnName:String
   *   EntitiesTrainingSetSize:Int
   *   NonEntitiesTrainingSetSize:Int
   *   classifier:Classifier
   *    ==>
   *   classificationModel:ClassificationModel
   * Applying Model
   *   TextsToQualify: Dataframe, TermColumName:String
   *   >> DataFrame ++ LikelyhoodColumn()
   
}*/



trait TermLikelyhoodEvaluatorBase extends Params with HasProbabilityCol with HasFeaturesCol {
    final val vectors = new Param[DataFrame](this, "vectors", "the vectors dictionary (dataframe)")
    final val entities = new Param[DataFrame](this, "entities", "the entitites dictionary (dataframe)")
    final val wordColumnNameVectors = new Param[String](this, "wordColumnNameVectors", "the column name of the 'words' in the vector dataframe")
    final val vectorColumnNameVectors = new Param[String](this, "vectorColumnNameVectors", "the column name of the word embeddings in the vector dataframe")
    final val entitiesColumnName = new Param[String](this, "entitiesColumnName", "the column name of the entities in the entities dataframe")
    final val labelColumnName = new Param[String](this, "labelColumnName", "the column name for the labels for the training")
    final val entitiesTrainingSetSize = new Param[Int](this, "entitiesTrainingSetSize", "the number of entities to train on (positive labels)")
    final val nonEntititesTrainingSetSize = new Param[Int](this, "nonEntitiesTrainingSetSize", "the number of non-entities to train on (negative labels)")
    final val manualTrainingData = new Param[DataFrame](this, "manualTrainingData", "a manually created dataframe to specify some values to train on")
    //final val inputColumnTransform = new Param[String](this, "inputColumnTransform", "the column name for the input data to transform (classification)")
    //final val outputColumnTransform = new Param[String](this, "inputColumnTransform", "the column name for the output of the transform (classification)")
    final val maxIter = new Param[Int](this, "maxIter", "The maximum number of iteration when calculating the constants A, B for determining the probabilities from scores")
    final val minstep = new Param[Double](this, "minstep", "The minimum step size when calculating the constants A, B for determining the probabilities from scores")
    final val sigma = new Param[Double](this, "sigma", "Sigma constant in hessian deriative when calculating the constants A, B for determining the probabilities from scores")
    final val lucene_maxLevDistance = new Param[Int](this, "lucene_maxLevDistance", "The maximum levensthein distance for getting the vectors for each query consisting of more than one word when creating the training set using lucene lookup")
    final val lucene_boostAcronyms = new Param[Boolean](this, "lucene_boostAcronyms", "If to use boost acronyms (two letters) for getting the vectors for each query consisting of more than one word when creating the training set using lucene lookup")
    final val lucene_indexPath = new Param[String](this, "lucene_indexPath", "The index path for getting the vectors for each query consisting of more than one word when creating the training set using lucene lookup")
    final val lucene_minScore = new Param[Double](this, "lucene_minScore", "The minimum score for getting the vectors for each query consisting of more than one word when creating the training set using lucene lookup")
    final val lucene_strategy = new Param[String](this, "lucene_strategy", "The strategy for getting the vectors for each query consisting of more than one word when creating the training set using lucene lookup")
    final val lucene_strategyParams = new Param[Map[String, String]](this, "lucene_strategyParams", "The strategy parameters for getting the vectors for each query consisting of more than one word when creating the training set using lucene lookup")


    setDefault(wordColumnNameVectors -> "name", vectorColumnNameVectors -> "vectors", labelColumnName ->"isEntity", entitiesTrainingSetSize ->1000, nonEntititesTrainingSetSize -> 1000,
               maxIter -> 100, minstep -> 1e-10, sigma -> 1e-12, lucene_maxLevDistance -> 0, lucene_indexPath -> "hdfs:///data/geo/glove_word_vectors.parquet.index", lucene_boostAcronyms -> false,
               lucene_minScore -> 0.0, lucene_strategy -> "demy.mllib.index.StandardStrategy", lucene_strategyParams -> Map.empty[String,String])

    def setVectors(value: DataFrame): this.type = set(vectors, value)
    def setEntities(value: DataFrame): this.type = set(entities, value)
    def setWordColumnNameVectors(value: String): this.type = set(wordColumnNameVectors, value)
    def setVectorColumnNameVectors(value: String): this.type = set(vectorColumnNameVectors, value)
    def setEntitiesColumnName(value: String): this.type = set(entitiesColumnName, value)
    def setLabelColumnName(value: String): this.type = set(labelColumnName, value)
    def setEntitiesTrainingSetSize(value: Int): this.type = set(entitiesTrainingSetSize, value)
    def setNonEntititesTrainingSetSize(value: Int): this.type = set(nonEntititesTrainingSetSize, value)
    def setManualTrainingData(value: DataFrame): this.type = set(manualTrainingData, value)
    //def setInputColumnTransform(value: String): this.type = set(inputColumnTransform, value)
    //def setOutputColumnTransform(value: String): this.type = set(outputColumnTransform, value)
    def setMaxIter(value: Int): this.type = set(maxIter, value)
    def setMinstep(value: Double): this.type = set(minstep, value)
    def setSigma(value: Double): this.type = set(sigma, value)
    def setLuceneMaxLevDistance(value: Int): this.type = set(lucene_maxLevDistance, value)
    def setLuceneBoostAcronyms(value: Boolean): this.type = set(lucene_boostAcronyms, value)
    def setLuceneIndexPath(value: String): this.type = set(lucene_indexPath, value)
    def setLuceneMinScore(value: Double): this.type = set(lucene_minScore, value)
    def setLuceneStrategy(value: String): this.type = set(lucene_strategy, value)
    def setLuceneStrategyParams(value: Map[String, String]): this.type = set(lucene_strategyParams, value)

    def transformSchema(schema: StructType): StructType = schema // takes schema and adds outputCol
//    override def transformSchema(schema: StructType): StructType = schema // takes schema and adds outputCol
    def copy(extra: ParamMap): this.type = {defaultCopy(extra)}

};class TermLikelyhoodEvaluator(override val uid: String) extends  Estimator[TermLikelyhoodEvaluatorModel] with TermLikelyhoodEvaluatorBase {
   def this() = this(Identifiable.randomUID("TermLikelyhoodEvaluatorModel"))

  /**
   * Model Training:
   *   Vector Dictionary: vectors:DataFrame, WordColumnName:String, VectorColumnName:String
   *   EntitiesTrainingSetSize:Int
   *   NonEntitiesTrainingSetSize:Int
   *   classifier:Classifier
   *    ==>
   *   classificationModel:ClassificationModel
   * Applying Model
   *   TextsToQualify: Dataframe, TermColumName:String
   *   >> DataFrame ++ LikelyhoodColumn()
   */

    override def fit(dataset: Dataset[_]): TermLikelyhoodEvaluatorModel = {

        import dataset.sparkSession.implicits._

        val N_vectors = get(vectors).get.count
        val N_entities = get(entities).get.count

        // drop duplicates + take only word vectors when the word consists of only 1 term (should be always the case normally)
        val entitiesUnique = get(entities).get.dropDuplicates(getOrDefault(entitiesColumnName))
                            .withColumn("temp", split(col(getOrDefault(entitiesColumnName)), "\\s+"))
                            .withColumn(getOrDefault(entitiesColumnName), lower(col(getOrDefault(entitiesColumnName)))) // to lower case, because words in vectors are lowercase too
                            .select(getOrDefault(entitiesColumnName), "temp")

        // For all entities that are composed of several words, calculate their mean word embedding
        val entitiesMoreWords = entitiesUnique.where(size(col("temp")) > 1)
                        .luceneLookup(right = get(vectors).get
                            , query = col("temp")//col("query")
                            , popularity = None // Some($"pop")
                            , text = col(get(wordColumnNameVectors).get)
                            , rightSelect= Array(col(get(vectorColumnNameVectors).get))
                            , leftSelect= Array(col("*"))
                            , maxLevDistance=getOrDefault(lucene_maxLevDistance)
                            , indexPath=getOrDefault(lucene_indexPath)
//                            , indexPath=Storage.getLocalStorage.getTmpPath()//"hdfs:///data/geo/all-cities-test-test-withoutOneLetter.parquet.index"
                            , reuseExistingIndex = true// true //false
                            , indexPartitions = 1
                            , maxRowsInMemory=1024
                            , indexScanParallelism = 2
                            , tokenizeRegex=Some("[^\\p{L}]+")
                            , minScore = getOrDefault(lucene_minScore)
                            //, boostAcronyms=true
                            , strategy=getOrDefault(lucene_strategy)
                            , strategyParams=getOrDefault(lucene_strategyParams)
                        )
                        .withColumn("array", udf ( (arr:Seq[Any]) => arr.map(struct => struct match {case r:Row => r.getAs[MLVector](0) case _ => throw new Exception(s"no match:!!! ${struct.getClass.getName}")})).apply(col("array")))
                        .withColumn("array", udf ( (array:Seq[MLVector]) =>
                                                array match { case vec:Seq[MLVector] => if (vec.contains(null)) null else vec
                                                              case _ => throw new Exception(s"NOO")}).apply(col("array")))
                        .filter(col("array").isNotNull)
                        .withColumn(getOrDefault(labelColumnName), lit(true))
                        .withColumn(getOrDefault(wordColumnNameVectors), lower(udf ( (arr:Seq[String]) => arr.mkString("_")).apply(col("temp"))))
                        .withColumn(getOrDefault(vectorColumnNameVectors), udf ( (array:Seq[MLVector]) => {
                                                val N = array.size
                                                val sumArray = array.reduce( (vec1, vec2) => vec1.sum(vec2) )
                                                Vectors.dense(sumArray.toArray.map(value => value / N))
                                            }).apply(col("array")))
                        .select(getOrDefault(wordColumnNameVectors), getOrDefault(vectorColumnNameVectors), getOrDefault(labelColumnName))


        var trainingData = get(vectors).get
                                      .join(entitiesUnique.where(size(col("temp")) === 1) , entitiesUnique.col(getOrDefault(entitiesColumnName)) === get(vectors).get.col(getOrDefault(wordColumnNameVectors)), "left")
                                      .withColumn(getOrDefault(labelColumnName), entitiesUnique.col(getOrDefault(entitiesColumnName)).isNotNull)
                                      .withColumn("isEntity", entitiesUnique.col("name").isNotNull)
                                      .select(getOrDefault(wordColumnNameVectors), getOrDefault(vectorColumnNameVectors), getOrDefault(labelColumnName))
                                      .union(entitiesMoreWords) // add entities consisting of more words (mean of their word vectors)
                                      .orderBy(rand())
                                      .withColumn("rank", row_number().over(Window.partitionBy(col(getOrDefault(labelColumnName))).orderBy(getOrDefault(labelColumnName)))) // row_number: gives number 1 to N for each different value in labelColumnName
                                      .where(col("rank") <= getOrDefault(entitiesTrainingSetSize) && col(getOrDefault(labelColumnName)) === true || col("rank") <= getOrDefault(nonEntititesTrainingSetSize) && col(getOrDefault(labelColumnName)) === false)
                                      .select(getOrDefault(wordColumnNameVectors), getOrDefault(vectorColumnNameVectors), getOrDefault(labelColumnName))
                                      .withColumn(getOrDefault(labelColumnName), udf( (label:Boolean) => label match { case false => 0 case true => 1} ).apply(col(getOrDefault(labelColumnName))))
                                      .cache() // puts dataframe into cache after first execution



        // add the manualTrainingData if provided
        if (!get(manualTrainingData).isEmpty) trainingData = trainingData.union(get(manualTrainingData).get)

        val Array(training, test) = trainingData.randomSplit(Array(0.75, 0.25), seed = 0)

        println("Define model..")
        val model = new LinearSVC()
                                .setFeaturesCol(getOrDefault(vectorColumnNameVectors))
                                .setLabelCol(getOrDefault(labelColumnName))

/*
        val paramGrid = new ParamGridBuilder()
                              .addGrid(model.regParam, Array(0.1, 0.01))
                              //.addGrid(model.elasticNetParam, Array(0.0, 0.5, 1.0))
                              .addGrid(model.maxIter, Array(300))
                              .build()

        val trainValidationSplit = new TrainValidationSplit()
                                          .setEstimator(model)
                                          .setEvaluator(new BinaryClassificationEvaluator().setLabelCol(getOrDefault(labelColumnName)).setMetricName("areaUnderROC"))
                                          .setEstimatorParamMaps(paramGrid)
                                          .setTrainRatio(0.8) // 80% of the data will be used for training and the remaining 20% for validation.
*/

        // Run train validation split, and choose the best set of parameters.
        //val model_trained = trainValidationSplit.fit(training)
        println("Fit model..")
        val model_trained = model.fit(training)

        val predictions = model_trained.transform(test).cache//.select(wordColumnNameVectors, labelColumnName, "prediction")
        (predictions.select(expr(s"sum(case when `${getOrDefault(labelColumnName)}` = 1 and prediction = 1 then 1 else 0 end) as TP")
                          , expr(s"sum(case when `${getOrDefault(labelColumnName)}` = 0 and prediction = 0 then 1 else 0 end) as TN")
                          , expr(s"sum(case when `${getOrDefault(labelColumnName)}` = 1 and prediction = 0 then 1 else 0 end) as FN")
                          , expr(s"sum(case when `${getOrDefault(labelColumnName)}` = 0 and prediction = 1 then 1 else 0 end) as FP"))
                    .select(expr(s"(TP+TN)/(TP+FP+FN+TN) as Accuracy")
                          , expr(s"TP/(TP+FP) as Precision")
                          , expr("TP/(TP+FN) as Recall"))
                    .show())

        // Calculating constants A, B to approximate prediction probabilities from the scores
        val A_B = getA_B(deci = predictions.toDF.map(r => r.getAs[MLVector]("rawPrediction")(0)).collect(),
                             label = predictions.toDF.map(r => r.getAs[Double]("prediction") match {case 0.0 => -1 case 1.0 => 1}).collect(), // Calculation requires binary labels: -1, 1
                             prior1=predictions.filter(col("prediction") === 1.0).count().toInt,
                             prior0=predictions.filter(col("prediction") === 0.0).count().toInt,
                             maxIter=getOrDefault(maxIter), minstep=getOrDefault(minstep), sigma=getOrDefault(sigma))


        // releases dataframe in cache
        trainingData.unpersist()
        predictions.unpersist()
        println("Constant A: "+A_B._1)
        println("Constant B: "+A_B._2)

        new TermLikelyhoodEvaluatorModel(uid = this.uid, trainedModel = model_trained, A = A_B._1, B = A_B._2)
    }

    def scores_to_proba(predictions:Dataset[_], rawScoresColumnName:String="rawPrediction", scoresColumnName:String="prediction", probaColumnName:String="probability", cA:Double= -1e-18, cB:Double= -1e-18, maxIter:Int=100, minstep:Double=1e-10, sigma:Double=1e-12) = {
        /*
            Only for testing puposes:

            SVM scores (distance to hyperplane) are transformed to probabilites using Platt's scaling (https://www.cs.colorado.edu/~mozer/Teaching/syllabi/6622/papers/Platt1999.pdf)
            Improved version of Platt's algorithm is implemented : https://www.csie.ntu.edu.tw/~cjlin/papers/plattprob.pdf

            Input setting
            --------------------------------------
            predictions : Dataset
            rawScoresColumnName : Containing the raw scores (distance to hyperplane)
            scoresColumnName : Contains the binary scores
            cA : Constant for the sigmoid function to calculate the probabilty from the score (If default -> calculate the constant )
            cB : Constant for the sigmoid function to calculate the probabilty from the score (If default -> calculate the constant )
            probaColumnName : Column name in which to store the calculated probability

            Parameter setting
            maxiter : Maximum number of iterations
            minstep : Minimum step taken in line search
            sigma : Set to any value > 0
        */
        import predictions.sparkSession.implicits._
        // if constants A, B not provided calculate them for the sigmoid function  P(y=1 | scores)
        var B:Double = cB
        var A:Double = cA
        if ( (cA == -1e-18) && (cB == -1e-18) ) {
            val A_B = getA_B(deci = predictions.toDF.map(r => r.getAs[MLVector](rawScoresColumnName)(0)).collect(),
                             label = predictions.toDF.map(r => r.getAs[Double](scoresColumnName) match {case 0.0 => -1 case 1.0 => 1}).collect(), // Calculation requires binary labels: -1, 1
                             prior1=predictions.filter(col(scoresColumnName) === 1.0).count().toInt,
                             prior0=predictions.filter(col(scoresColumnName) === 0.0).count().toInt,
                             maxIter=maxIter, minstep=minstep, sigma=sigma)
            A = A_B._1
            B = A_B._2
        }

        predictions.withColumn(probaColumnName, udf((rawPreds:Seq[MLVector]) =>{
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
    }



    def getA_B(deci:Array[Double], label:Array[Int], prior1:Int, prior0:Int, maxIter:Int=100, minstep:Double=1e-10, sigma:Double=1e-12): (Double, Double) = {
        /*
            SVM scores (distance to hyperplane) are transformed to probabilites using Platt's scaling (https://www.cs.colorado.edu/~mozer/Teaching/syllabi/6622/papers/Platt1999.pdf)
            Improved version of Platt's algorithm is implemented : https://www.csie.ntu.edu.tw/~cjlin/papers/plattprob.pdf

            This function calculates the two constant parameters A,B for the sigmoid function P(y=1 | scores)

            Input parameters
            -------------------------------------
            deci : array of SVM decision values
            label :  array of booleans: is the example labeled +1?
            prior1 : number of positive examples
            prior0 : number of negative examples

            Parameter setting
            maxiter : Maximum number of iterations
            minstep : Minimum step taken in line search
            sigma : Set to any value > 0
        */


        //Construct initial values: target support in array t, initial function value in fval
        val hiTarget=(prior1+1.0)/(prior1+2.0)
        val loTarget=1.0/(prior0+2.0)
        val len=prior1+prior0 // Total number of data

        val t = label.map {value => if (value > 0 ) hiTarget else loTarget }

        var A=0.0
        var B=Log((prior0+1.0)/(prior1+1.0))

        var fval = ((0 to len-1).map { i => {
            val fApB = deci(i)*A + B
            if ( fApB >= 0) t(i)*fApB + Log(1+exp(-fApB))
            else (t(i)-1)*fApB + Log(1+exp(fApB))
        }}.sum )

        var isTrue = true
        var it = 1

        while( (isTrue == true) && (it <= maxIter)) {
            //Update Gradient and Hessian (use H’ = H + sigma I)
            var h11:Double = sigma
            var h22:Double = sigma
            var h21:Double = 0.0
            var g1:Double = 0.0
            var g2:Double = 0.0

            var p:Double = 0.0
            var q:Double = 0.0
            var d1:Double = 0.0
            var d2:Double = 0.0
            for ( i <- 0 to len-1) {
                var fApB=deci(i)*A + B
                if (fApB >= 0) {
                    p=exp(-fApB)/(1.0+exp(-fApB))
                    q=1.0/(1.0+exp(-fApB))
                }
                else {
                    p=1.0/(1.0+exp(fApB))
                    q=exp(fApB)/(1.0+exp(fApB))
                }
                d2=p*q
                h11 += deci(i)*deci(i)*d2
                h22 += d2
                h21 += deci(i)*d2
                d1=t(i)-p
                g2 += d1
                g1 += deci(i)*d1

            }
            if (Math.abs(g1)<1e-5 && Math.abs(g2)<1e-5) //Stopping criteria
                isTrue = false

            //Compute modified Newton directions
            else {
                var det=h11*h22-h21*h21
                var dA = -(h22*g1-h21*g2)/det
                var dB = -(-h21*g1+h11*g2)/det
                var gd  =g1*dA+g2*dB
                var stepsize:Double = 1
                var isSearch = true
                while ( (stepsize >= minstep) && (isSearch == true)){ //Line search
                    var newA = A+stepsize*dA
                    var newB = B+stepsize*dB
                    var newf= ( (0 to len-1).map { i => {
                        val fApB = deci(i)*newA + newB
                        if ( fApB >= 0) t(i)*fApB + Log(1+exp(-fApB))
                        else (t(i)-1)*fApB + Log(1+exp(fApB))
                    }}.sum )
                    if (newf < fval  +0.0001*stepsize*gd){
                        A=newA
                        B=newB
                        fval=newf
                        isSearch = false
                        println("Sufficient decrease satisfied")
                        //break //Sufficient decrease satisfied
                    }
                    else
                        stepsize = stepsize / 2.0
                }
                //println("stepsize: "+stepsize+"!! minsize: "+minstep)
                if (stepsize < minstep){
                    println("Line search fails")
                    isTrue = false
                }
            }

            it = it + 1
        }

        if (it >= maxIter) println("Reaching maximum iterations")

        (A,B)
    }



}

class TermLikelyhoodEvaluatorModel(override val uid: String, trainedModel:LinearSVCModel, A:Double, B:Double) extends org.apache.spark.ml.Model[TermLikelyhoodEvaluatorModel] with TermLikelyhoodEvaluatorBase with HasRawPredictionCol with HasProbabilityCol {

    def this(trainedModel:LinearSVCModel, A:Double, B:Double) = this(Identifiable.randomUID("TermLikelyhoodEvaluatorModel"), trainedModel, A, B)
    val temp = trainedModel
    val tempA = A
    val tempB = B

    override def transformSchema(schema: StructType): StructType = {
        schema.add(StructField("probability", ArrayType(DoubleType, false), true ))
              .add(StructField("rawPrediction", ArrayType(elementType=(new AttributeGroup(name="dummy")).toStructField.dataType, containsNull = true) ))
    }

    def transform(dataset: Dataset[_]):DataFrame = {
        import dataset.sparkSession.implicits._
        val df = dataset.toDF
        val outSchema = transformSchema(df.schema)

        val rdd = df.rdd.mapPartitions(iter => {
            val method = trainedModel.getClass.getDeclaredMethod("predictRaw", classOf[MLVector])
            method.setAccessible(true)
            val rawPredicter = (trainedModel, method)
            iter.map{ r =>
                  val newValues = r.toSeq ++ ({
                    val wordVectors = r.getAs[Seq[MLVector]](r.size-1)

                    val rawPredictions = wordVectors.map{ vec => if( vec != null ) method.invoke(trainedModel, vec).asInstanceOf[MLVector] else null}
                    val probabilities = rawPredictions.map{ rawPred =>
                                            if (rawPred != null) {
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
                                            } else 0.0


                                         }.toArray

                    Seq(probabilities, rawPredictions)
                  })
                  Row.fromSeq(newValues)
            }
          })

        dataset.sparkSession.createDataFrame(rdd, outSchema)
    }

}
