package demy.mllib.text

import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.{Param, ParamMap}
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.{Dataset, DataFrame}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.{udf, col, explode}

class SentenceBuilder(override val uid: String) extends Transformer {
    final val inputCol = new Param[String](this, "inputCol", "The input column")
    final val outputCol = new Param[String](this, "outputCol", "The new column column")
    final val explodeSentences = new Param[Boolean](this, "explodeSentences", "If each sentence shoud be on a separte line or on an single array of strings ")
    final val phraseIdCol = new Param[String](this, "phraseIdCol", "The phraseId column")
    final val minPhraseSize = new Param[Int](this, "minPhraseSize", "The phrase min size (bigger or equal than 0)")
    final val maxPhraseSize = new Param[Int](this, "maxPhraseSize", "The phrase max size (bigger or equal than min phrase size)")
    def setInputCol(value: String): this.type = set(inputCol, value)
    def setOutputCol(value: String): this.type = set(outputCol, value)
    def setPhraseIdCol(value: String): this.type = set(phraseIdCol, value)
    def setMinPhraseSize(value: Int): this.type = set(minPhraseSize, value)
    def setMaxPhraseSize(value: Int): this.type = set(maxPhraseSize, value)
    setDefault(explodeSentences -> true)
    private def splitSentence(text:String) = {
        if(text==null) 
            Array("")
        else {
            val splits = scala.collection.mutable.ArrayBuffer(0)
            val hardBreak = Seq('\n', '\r', '\t')
            val softBreak = Seq('.', ';', ':', '!', '?')
            val wordSep = Seq(' ', '(', ')', '(', ')')
            val startWords = Seq("je", "tu", "il", "elle", "vous", "nous", "ils")
            var lastBreak = 0
            var inWord = false
            var wordCount = 0
            Range(0, text.size).foreach(i =>{
                var doBreak = false
                var wasInWord = inWord
                val c = text(i)
                if(hardBreak.contains(c)) {
                    doBreak = true
                    inWord = false
                } else if(softBreak.contains(c)) {
                    if(wordCount>get(minPhraseSize).get) doBreak = true
                    inWord = false
                } else if(wordSep.contains(c)) {
                    if(wordCount>get(maxPhraseSize).get) doBreak = true
                    inWord = false
                } else if(!wasInWord && startWords.filter(startWord => (
                                                                        startWord == text.slice(i, i + startWord.length).toLowerCase //le mot debut de phrase suit à continuation
                                                                        && text.size > i + startWord.length && wordSep.contains(text(i + startWord.length)) //le mot debut de phrase est suivi par un espace)
                                                                    )).size>0) {
                if(wordCount>get(minPhraseSize).get) doBreak = true
                    inWord = true
                } else inWord = true
            
                if(doBreak) {
                    if(lastBreak<i-1) splits += i
                        lastBreak = i
                wordCount = 0
                } else if (!inWord && wasInWord)
                    wordCount = wordCount + 1
            })
            if(splits.size > 0 && text.slice(splits.last, text.size).split("\\W").filter(w => w.size > 0).size < get(minPhraseSize).get ) splits.remove(splits.size-1) //Removing last split if las sentence is shorter than minPhraseSize
            splits.toArray.zipWithIndex.map(p => p match { case (phraseStart, i) => {
                                                              val phraseEnd = if(i+1 == splits.size) text.size else splits(i+1)
                                                              text.slice(phraseStart, phraseEnd)
                                             }})
        }}
    override def transform(dataset: Dataset[_]): DataFrame = {
        if(getOrDefault(explodeSentences)) {
            dataset.withColumn("__temporary__", explode(udf((text:String)=>splitSentence(text).zipWithIndex).apply(col(get(inputCol).get))))
                .drop(get(inputCol).get)
                .withColumn(get(outputCol).get, col("__temporary__._1"))
                .withColumn(get(phraseIdCol).get, col("__temporary__._2"))
                .drop("__temporary__")
        } else {
            dataset.withColumn(get(outputCol).get, udf((text:String)=>splitSentence(text)).apply(col(get(inputCol).get)))
                .drop(get(inputCol).get)
        }
    }
    override def transformSchema(schema: StructType): StructType = {
        if(getOrDefault(explodeSentences))
            StructType(schema.fields.filter(f => f.name != get(inputCol).get)).add(StructField(get(outputCol).get, StringType )).add(StructField(get(phraseIdCol).get, IntegerType))
        else
            StructType(schema.fields.filter(f => f.name != get(inputCol).get)).add(StructField(get(outputCol).get, ArrayType(elementType=StringType, containsNull = true)))
    }
    def copy(extra: ParamMap): SentenceBuilder = {defaultCopy(extra)}    
    def this() = this(Identifiable.randomUID("SentenceBuilder"))
};object SentenceBuilder {
}
