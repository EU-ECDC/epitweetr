package demy.mllib.params

import org.apache.spark.sql.Dataset
import org.apache.spark.ml.param.{Param, ParamValidators, Params}
import scala.concurrent.ExecutionContext
import java.util.concurrent.Executors
import org.apache.spark.ml.param.shared

trait HasParallelismDemy extends Params {
  val parallelism = new Param[Int](this, "parallelism", "the number of threads to use when running parallel algorithms", ParamValidators.gtEq(1))
  setDefault(parallelism -> 1)
  def getParallelism: Int = getOrDefault(parallelism)
  def getExecutionContext: ExecutionContext = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(getParallelism))
  def setParallelism(value: Int): this.type = set(parallelism, value)
}

trait HasLabelCol extends shared.HasLabelCol {
  def setLabelCol(value: String): this.type = set(labelCol, value)
}
trait HasFeaturesCol extends shared.HasFeaturesCol {
  def setFeaturesCol(value: String): this.type = set(featuresCol, value)
}

trait HasScoreCol extends Params {
  val scoreCol = new Param[String](this, "scoreCol", "The name of the score column to produce")
  def getScoreCol:String = getOrDefault(scoreCol)
  def setScoreCol(value: String): this.type = set(scoreCol, value)
  setDefault(scoreCol-> "score")
}

trait HasExecutionMetrics extends Params {
  val logMetrics = new Param[Boolean](this, "logMetrics", "If this step should log execution metrics")
  val metricsToLog = new Param[Array[String]](this, "metricsToLog", "the metrics to log (all by default)")
  setDefault(logMetrics -> false, metricsToLog -> Array[String]())
  def setLogMetrics(value: Boolean): this.type = set(logMetrics, value)
  def setMetricsToLog(value:Array[String]): this.type = set(metricsToLog, value)
  def getLogMetrics: Boolean = getOrDefault(logMetrics)
  def getMetricsToLog: Array[String] = getOrDefault(metricsToLog)
  val metrics:scala.collection.mutable.Map[String, Double] = scala.collection.mutable.Map[String, Double]()
}

trait HasFolds extends Params {
  val numFolds = new Param[Int](this, "numFolds", "The number of random folds to build")
  def setNumFolds(value: Int): this.type = set(numFolds, value)
}

trait HasTrainRatio extends Params {
  val trainRatio = new Param[Double](this, "trainRatio", "The train set ratio as a proportion e.g. 0.75 meand that 3/4 of randomly selected rows will be used for training (not compatible folds > 1")
  def setTrainRatio(value: Double): this.type = set(trainRatio, value)
}

trait HasInputCol extends shared.HasInputCol {
  def setInputCol(value: String): this.type = set(inputCol, value)
}

trait HasInputCols extends shared.HasInputCols {
  def setInputCols(value: Array[String]): this.type = set(inputCols, value)
}

trait HasOutputCol extends shared.HasOutputCol {
  def setOutputCol(value: String): this.type = set(outputCol, value)
}

trait HasRawPredictionCol extends shared.HasRawPredictionCol {
  def setRawPredictionCol(value: String): this.type = set(rawPredictionCol, value)
}

trait HasPredictionCol extends shared.HasPredictionCol {
  def setPredictionCol(value: String): this.type = set(predictionCol, value)
}

trait HasGroupByCols extends Params {
  val groupByCols = new Param[Array[String]](this, "groupByCols", "The columns to group by")
  def setGroupByCols(value: Array[String]): this.type = set(groupByCols, value)
}

trait HasStratifyByCols extends Params {
  val stratifyByCols = new Param[Array[String]](this, "stratifyByCols", "The columns to group by")
  def setStratifyByCols(value: Array[String]): this.type = set(stratifyByCols, value)
}

trait HasProbabilityCol extends shared.HasProbabilityCol {
  def setProbabilityCol(value: String): this.type = set(probabilityCol, value)
}


trait HasTokensCol extends Params {
  val tokensCol = new Param[String](this, "tokensCol", "Column containing input arrays of tokens")
  def setTokensCol(value: String): this.type = set(tokensCol, value)
}
trait HasVectorsCol extends Params {
  val vectorsCol = new Param[String](this, "vectorCol", "Column containing input arrays of vectors (usually associated with tokens array)")
  def setVectorsCol(value: String): this.type = set(vectorsCol, value)
}

trait HasSeed extends shared.HasSeed {
  def setSeed(value:Long): this.type = set(seed, value)
}
