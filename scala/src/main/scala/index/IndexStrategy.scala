package demy.mllib.index;

import org.apache.lucene.search.{IndexSearcher, TermQuery, BooleanQuery, FuzzyQuery, BoostQuery}
import org.apache.lucene.search.BooleanClause.Occur
import org.apache.lucene.index.{DirectoryReader, Term}
import org.apache.lucene.queries.function.FunctionQuery
import org.apache.lucene.queries.function.valuesource.DoubleFieldSource
import org.apache.lucene.document.Document
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.apache.lucene.document.{Document, TextField, StringField, IntPoint, BinaryPoint, LongPoint, DoublePoint, FloatPoint, Field, StoredField}
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import java.io.{ObjectInputStream,ByteArrayInputStream}
import scala.collection.JavaConverters._
import demy.storage.{Storage, LocalNode}
import demy.util.{log => l}

case class Ngram(terms:Array[String], startIndex:Int, endIndex:Int, termWeights:Seq[Double])
object Ngram {
  def apply(terms:Array[String], startIndex:Int, endIndex:Int):Ngram = Ngram(terms=terms, startIndex=startIndex,endIndex=endIndex, termWeights= terms.map(_ => 1.0))
  def apply(terms:Array[String]):Ngram = Ngram(terms=terms, startIndex=0,endIndex=terms.size, termWeights= terms.map(_ => 1.0))
  def apply(terms:Array[String], termWeights:Seq[Double]):Ngram = Ngram(terms=terms, startIndex=0,endIndex=terms.size, termWeights= terms.map(_ => 1.0))
  //def print() {println("Terms: "+terms.mkString(",")+" startIndex: "+startIndex+" endIndex: "+endIndex+"\nWeights:"+termWeights.mkString(","))}
}

case class SearchMatch(docId:Int, score:Float, ngram:Ngram)

trait IndexStrategy {
  val searcher:IndexSearcher
  val indexDirectory:LocalNode
  val reader:DirectoryReader

  def setProperty(name:String, value:String) : IndexStrategy
  def set(searcher:IndexSearcher, indexDirectory:LocalNode,reader:DirectoryReader) : IndexStrategy

  /**
   * Method that will call weightevaluateNgram to find documents match within a text. This is the entry point for heuristics that will find occurrences on terms subsets
   */
  def searchDoc(
    terms:Array[String]
    , maxHits:Int
    , maxLevDistance:Int=2
    , filter:Row = Row.empty
    , usePopularity:Boolean
    , minScore:Double=0.0
    , boostAcronyms:Boolean=false
    , termWeights:Option[Seq[Double]]=None
    , caseInsensitive:Boolean = true
    , tokenize:Boolean = true
    ) = {
      evaluate(
        terms = terms
        , likelihood = termWeights.getOrElse(Array.fill(terms.size)(1.0)) 
        , from = 0
        , to = terms.size
        , maxHits=maxHits
        , maxLevDistance=maxLevDistance
        , filter=filter
        , usePopularity = usePopularity
        , minScore = minScore
        , boostAcronyms = boostAcronyms
        , caseInsensitive = caseInsensitive
      )
  }

  /***
   * Method for evaluating a particular Ngram, all terms are expected to be evaluated
   */
  def evaluate(terms:Array[String], likelihood:Seq[Double], from:Int, to:Int, maxHits:Int, maxLevDistance:Int=2, filter:Row = Row.empty, usePopularity:Boolean
                     , minScore:Double=0.0 ,boostAcronyms:Boolean=false, caseInsensitive:Boolean = true
                     , minTokenLikehood:Double = 0.4
    ): Array[SearchMatch] = {
    val qb = new BooleanQuery.Builder() //  combines multiple TermQuery instances into a BooleanQuery with multiple BooleanClauses, where each clause contains a sub-query and operator
    val b = new BooleanQuery.Builder()
    for(i <- Iterator.range(from, to)) { 
      val s= terms(i)
      val weight = likelihood(i)

      var allLetterUppercase = s.size == 2 && Range(0, s.length).forall(ind => s(ind).isUpper)
      if (allLetterUppercase && boostAcronyms) {
        // if term is only in Uppercase -> double term: "TX" -> "TXTX" (ensures that term is not neglected due to too less letters)
        b.add(new BoostQuery(new TermQuery(new Term("_text_", s+s)), if(maxLevDistance > 0) 15.00F else 4.00F), Occur.SHOULD) // High boosting factor to find doubles
      } else if(maxLevDistance>0) {
        // if query is fuzzy, two queries will be sent one in fuzzy and another boosting the perfect match 
        if(s.size > 2) b.add(new BoostQuery(new FuzzyQuery(new Term("_text_", if(caseInsensitive) s.toLowerCase else s), 1, maxLevDistance), weight.toFloat), Occur.SHOULD)
        b.add(new BoostQuery(new TermQuery(new Term("_text_",  if(caseInsensitive) s.toLowerCase else s)), 4.00F*weight.toFloat), Occur.SHOULD)
      } else if(weight < 0.8 && weight >= minTokenLikehood ) { 
        b.add(new BoostQuery(new TermQuery(new Term("_text_",  if(caseInsensitive) s.toLowerCase else s)), weight.toFloat), Occur.SHOULD)
      } else if(weight >= minTokenLikehood ) { 
        b.add(new TermQuery(new Term("_text_",  if(caseInsensitive) s.toLowerCase else s)), Occur.SHOULD)
      }
    }

    qb.add(b.build, Occur.MUST)
    
    if(filter.schema != null) {
       filter.schema.fields.zipWithIndex.foreach(p => p match { case (field, i) =>
          if(!filter.isNullAt(i)) field.dataType match {
          case dt:StringType => qb.add(new TermQuery(new Term(field.name, filter.getAs[String](i))), Occur.MUST)
          case dt:IntegerType => qb.add(IntPoint.newExactQuery("_point_"+field.name, filter.getAs[Int](i)), Occur.MUST)
          case dt:BooleanType => qb.add(BinaryPoint.newExactQuery("_point_"+field.name, Array(filter.getAs[Boolean](i) match {case true => 1.toByte case _ => 0.toByte})), Occur.MUST)
          case dt:LongType => qb.add(LongPoint.newExactQuery("_point_"+field.name, filter.getAs[Long](i)), Occur.MUST)
          case dt:FloatType => qb.add(FloatPoint.newExactQuery("_point_"+field.name, filter.getAs[Float](i)), Occur.MUST)
          case dt:DoubleType => qb.add(DoublePoint.newExactQuery("_point_"+field.name, filter.getAs[Double](i)), Occur.MUST)
          case dt => throw new Exception(s"Spark type {$dt.typeName} cannot be used as a filter since it has not been indexed")
       }})
    }

    val q = 
      if(usePopularity) org.apache.lucene.queries.function.FunctionScoreQuery.boostByValue(qb.build, org.apache.lucene.search.DoubleValuesSource.fromDoubleField("_pop_"))
      else qb.build


    //val startTime = System.nanoTime
    val docs = this.searcher.search(q, maxHits);
    //val endTime = System.nanoTime
    //if( ngram.termWeights!= null && ngram.termWeights.size>0)
    //    println(s"SEARCH ${(endTime - startTime)/1e6d} msecs for ${ngram.terms.size} terms ispeed: ${((endTime - startTime)/1e6d)/(ngram.terms.size)} ${ngram.terms.mkString(",")}")
    
    val hits = docs.scoreDocs;
    //hits

    //println(s"HITS ARE: ($from, $to)${terms.slice(from, to).toSeq}")
    hits.map(hit => SearchMatch(docId=hit.doc, score=hit.score, Ngram(terms=terms.slice(from, to), startIndex = from, endIndex = to)))

  }

  /**
   *  Method fthat will use searchDoc and transform the results into SparkRows
   */

  def search(tokens:Array[String], maxHits:Int, filter:Row = Row.empty, outFields:Seq[StructField]=Seq[StructField](),
             maxLevDistance:Int=2 , minScore:Double=0.0, boostAcronyms:Boolean=false, showTags:Boolean=false, usePopularity:Boolean, termWeights:Option[Seq[Double]]=None,
             caseInsensitive:Boolean = true):Array[GenericRowWithSchema] = {

    val outSchema = StructType(outFields.toList :+ StructField("_score_", FloatType)
                                                :+ StructField("_tags_", ArrayType(StringType))
                                                :+ StructField("_startIndex_", IntegerType)
                                                :+ StructField("_endIndex_", IntegerType))
                                              //  :+ StructField("_pos_", ArrayType(IntegerType, IntegerType)) ) // add fields
    //if(tokens != null) println(s"$termWeights, ${tokens.mkString(",")}")
    if (tokens != null && tokens.size >0 ) {
      searchDoc(
        terms = tokens, maxHits=maxHits, filter=filter, maxLevDistance=maxLevDistance
          ,minScore=minScore, boostAcronyms = boostAcronyms, usePopularity = usePopularity, termWeights=termWeights
          ,caseInsensitive = caseInsensitive
        )
        .flatMap(hit => {
          if(hit.score < minScore) None
          else {
            val doc = this.searcher.doc(hit.docId)
            Some(new GenericRowWithSchema(
              values = outFields.toArray.map(field => {
                val lucField = doc.getField(field.name)
                if(field.name == null || lucField == null) null
                else
                  field.dataType match {
                case dt:StringType => lucField.stringValue
                case dt:IntegerType => lucField.numericValue().intValue()
                case dt:BooleanType => lucField.binaryValue().bytes(0) == 1.toByte
                case dt:LongType =>  lucField.numericValue().longValue()
                case dt:FloatType => lucField.numericValue().floatValue()
                case dt:DoubleType => lucField.numericValue().doubleValue()
                case dt => {
                  var obj:Any = null
                  val serData= lucField.binaryValue().bytes;
                  if (serData!=null) {
                     val in=new ObjectInputStream(new ByteArrayInputStream(serData))
                     obj = in.readObject()
                     in.close()
                  }
                  obj
                }
              }}) ++ Array(hit.score, hit.ngram.terms, hit.ngram.startIndex, hit.ngram.endIndex)// add fields
              ,schema = outSchema))
          }
        })
    } 
      else Array[GenericRowWithSchema]()
  }


  def close(deleteSnapShot:Boolean = false) {
    SparkLuceneReader.readLock.synchronized {
      SparkLuceneReader.readerCount(this.indexDirectory.path) = SparkLuceneReader.readerCount(this.indexDirectory.path) - 1
      if(SparkLuceneReader.readerCount(this.indexDirectory.path) == 0) {
         SparkLuceneReader.readerCount.remove(this.indexDirectory.path)
         SparkLuceneReader.readers.remove(this.indexDirectory.path)
         this.reader.close
         this.reader.directory().close
         if(deleteSnapShot && this.indexDirectory.exists) {
           this.indexDirectory.deleteIfTemporary(recurse = true)
         }
      }
    
    }
  }


}
