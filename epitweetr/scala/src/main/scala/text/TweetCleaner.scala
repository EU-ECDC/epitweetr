package demy.mllib.text

import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.{Param, ParamMap}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Dataset, DataFrame}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, udf, lit}
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.ml.param.shared._
import java.util.regex.Pattern

class TweetCleaner(override val uid: String) extends Transformer with HasInputCol with HasOutputCol {
    def setInputCol(value: String): this.type = set(inputCol, value)
    def setOutputCol(value: String): this.type = set(outputCol, value)

    final val sentimentChars = new Param[String](this, "sentimentChars", "Parquet file containing a list of character sequences to be considered as emoticos/emoji including a column 'positiveScore')")
    final val positiveCharsTo = new Param[String](this, "positiveCharTo", "replacement for positive cahrs")
    final val negativeCharsTo = new Param[String](this, "negativeCharTo", "replacement for negative chars")
    final val userMentionTo = new Param[String](this, "userMentionTo", "replacement for use mentions")
    final val linkTo = new Param[String](this, "linkTo", "replacement for links")
    setDefault(positiveCharsTo->"great", negativeCharsTo-> "bad", userMentionTo->"user", linkTo->"link")
    def setSentimentChars(value: String): this.type = set(sentimentChars, value)
    def setPositiveCharsTo(value: String): this.type = set(positiveCharsTo, value)
    def setNegativeCharsTo(value: String): this.type = set(negativeCharsTo, value)
    def setUserMentionTo(value: String): this.type = set(userMentionTo, value)
    def setLinkTo(value: String): this.type = set(linkTo, value)
    override def transform(dataset: Dataset[_]): DataFrame = {
      val spark = dataset.sparkSession
      import spark.implicits._ 
      val url = "((https?|ftp|gopher|telnet|file|Unsure|http):((//)|(\\\\))+[\\w\\d:#@%/;$()~_?\\+-=\\\\\\.&]*)"
      val user = "(?<=^|(?<=[^a-zA-Z0-9-_\\.]))@(\\w+)\\b"
      val stop = "\\bRT\\b|\\bvia\b"
      val hash = "#"
      val replacements = spark.sparkContext.broadcast(Array(url -> (" "+getOrDefault(linkTo)+" "), user -> (" "+getOrDefault(userMentionTo)+" "), stop -> " ", hash -> "")
             ++ (get(sentimentChars) match {
                         case Some(path) => spark.read.parquet(path).as[(String, Int)]
                                        .map(p => (p._1, if(p._2 > 0) getOrDefault(positiveCharsTo) else if(p._2<0) getOrDefault(negativeCharsTo) else ""))
                                        .map(p => (Pattern.quote(p._1), " "+p._2+" "))
                                        .collect
                         case _ => Array[(String, String)]()
                 }))
      
      dataset.withColumn(get(outputCol).get, udf((text:String) => {
          replacements.value.foldLeft(text)((current, replacement)=> current.replaceAll(replacement._1, replacement._2))
       }).apply(col(get(inputCol).get)))
    }
    override def transformSchema(schema: StructType): StructType = {schema.add(StructField(get(outputCol).get, StringType))}
    def copy(extra: ParamMap): this.type = {defaultCopy(extra)}    
    def this() = this(Identifiable.randomUID("TweetCleaner"))
}
object TweetCleaner {
}
