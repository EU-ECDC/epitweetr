package demy.mllib.index;

import org.apache.lucene.search.{IndexSearcher, TermQuery, BooleanQuery, FuzzyQuery}
import org.apache.lucene.store.NIOFSDirectory
import org.apache.lucene.index.{DirectoryReader, Term}
import org.apache.lucene.search.BooleanClause.Occur
import org.apache.lucene.queries.function.FunctionQuery
import org.apache.lucene.queries.function.valuesource.DoubleFieldSource
import org.apache.lucene.search.BoostQuery
import org.apache.lucene.document.Document
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.apache.lucene.document.{Document, TextField, StringField, IntPoint, BinaryPoint, LongPoint, DoublePoint, FloatPoint, Field, StoredField}
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import java.io.{ObjectInputStream,ByteArrayInputStream}
import scala.collection.JavaConverters._
import demy.storage.LocalNode
import demy.util.log
import demy.util.implicits.IterableUtil

case class NgramStrategy(searcher:IndexSearcher, indexDirectory:LocalNode, reader:DirectoryReader, nNgrams:Int= 3) extends IndexStrategy {
  def this() = this(null, null, null)
  def setProperty(name:String,value:String) = {
    name match {
      case "nNgrams" => NgramStrategy(searcher = searcher, indexDirectory = indexDirectory,  reader = reader, nNgrams=value.toInt)
      case _ => throw new Exception(s"Not supported property ${name} on NgramReadStrategy")
    }
  }
  def set(searcher:IndexSearcher, indexDirectory:LocalNode,reader:DirectoryReader) =  NgramStrategy(searcher = searcher, indexDirectory = indexDirectory,reader = reader)

  // get expanded Ngram in left or right direction of the provided term
  def getNgram(currentNgram:Ngram, terms:Array[String], termWeights:Option[Seq[Double]], nNgrams:Int, direction:String="left"): Option[Ngram] = {

      val nTerms = terms.length

      // get ngram on left side
      if (direction == "left") {
          // if ngram is neither on the left nor the right border
          if ( (currentNgram.startIndex > 0) & (currentNgram.endIndex < nTerms) ) {
            val newStartIndex = currentNgram.startIndex-1
            val newEndIndex = currentNgram.endIndex
            termWeights match {
              case Some(weights) => Some(Ngram(terms=terms.slice(newStartIndex, newEndIndex), startIndex=newStartIndex, endIndex=newEndIndex, termWeights=weights.slice(newStartIndex, newEndIndex)))
              case None => Some(Ngram(terms=terms.slice(newStartIndex, newEndIndex), startIndex=newStartIndex, endIndex=newEndIndex))
            }
          }
          // left border
          else if (currentNgram.startIndex == 0)  None
          // right border
          else if (currentNgram.endIndex == nTerms) {
            val newStartIndex = currentNgram.startIndex-1
            val newEndIndex = currentNgram.endIndex
            termWeights match {
              case Some(weights) => Some(Ngram(terms=terms.slice(newStartIndex, newEndIndex), startIndex=newStartIndex, endIndex=newEndIndex, termWeights=weights.slice(newStartIndex, newEndIndex)))
              case None => Some(Ngram(terms=terms.slice(newStartIndex, newEndIndex), startIndex=newStartIndex, endIndex=newEndIndex))
            }
          }

          else None
      }
      // get ngram on right side
      else if (direction == "right") {
          // if ngram is neither on the left nor the right border
          if ( (currentNgram.startIndex > 0) & (currentNgram.endIndex < nTerms) ) {
            val newStartIndex = currentNgram.startIndex
            val newEndIndex = currentNgram.endIndex+1
            termWeights match {
              case Some(weights) => Some(Ngram(terms=terms.slice(newStartIndex, newEndIndex), startIndex=newStartIndex, endIndex=newEndIndex, termWeights=weights.slice(newStartIndex, newEndIndex)))
              case None => Some(Ngram(terms=terms.slice(newStartIndex, newEndIndex), startIndex=newStartIndex, endIndex=newEndIndex))
            }
          }

          // left border
          else if (currentNgram.startIndex == 0) {
            val newStartIndex = 0
            val newEndIndex = nNgrams+1
            termWeights match {
              case Some(weights) => Some(Ngram(terms=terms.slice(newStartIndex, newEndIndex), startIndex=newStartIndex, endIndex=newEndIndex, termWeights=weights.slice(newStartIndex, newEndIndex)))
              case None => Some(Ngram(terms=terms.slice(newStartIndex, newEndIndex), startIndex=newStartIndex, endIndex=newEndIndex))
            }
          }

          // right border
          else if (currentNgram.endIndex == nTerms)  None
          else  None

      } else {
          println("ERROR: Wrong direction provided in func getNgram(). Possible choices: [left, right]")
          None
      }
  }
  def evaluateNGram(ngram:Ngram, maxHits:Int, maxLevDistance:Int=2, filter:Row = Row.empty, usePopularity:Boolean
                     , minScore:Double=0.0 ,boostAcronyms:Boolean=false, caseInsensitive:Boolean = true
                     , minTokenLikehood:Double = 0.6
    ): Array[SearchMatch] = {
      evaluate(
        terms = ngram.terms
        , likelihood = ngram.termWeights
        , from = 0
        , to = ngram.terms.size
        , maxHits = maxHits
        , maxLevDistance = maxLevDistance
        , filter = filter
        , usePopularity = usePopularity
        , minScore = minScore
        , boostAcronyms = boostAcronyms
        , caseInsensitive = caseInsensitive
        , minTokenLikehood = minTokenLikehood
      ).map{case SearchMatch(docId, score, ng) => SearchMatch(docId, score, ngram)}
  }

  // search in expanded Ngrams until max score is found
  def searchNgramExpanded(maxScoreLocal:Array[SearchMatch], terms:Array[String], termWeights:Option[Seq[Double]]=None, nNgrams:Int, nTerms:Int
                            , maxHits:Int, maxLevDistance:Int=2, filter:Row = Row.empty , usePopularity:Boolean, minScore:Double=0.0, boostAcronyms:Boolean=false
                            , caseInsensitive:Boolean = true, tokenize:Boolean = true): Array[SearchMatch] = {

    var maxScoreLocalNgram = maxScoreLocal
    var ngramLocalLeft:Option[Ngram] = None
    var ngramLocalRight:Option[Ngram] = None
    var temp = Array[SearchMatch]()
    var temp2 = Array[SearchMatch]()

    var breakLoop = false

    ngramLocalLeft = getNgram(maxScoreLocalNgram.head.ngram, terms, termWeights, nNgrams, direction="left")
    ngramLocalRight = getNgram(maxScoreLocalNgram.head.ngram, terms, termWeights, nNgrams, direction="right")

    while ( !(ngramLocalLeft.isEmpty && ngramLocalRight.isEmpty) && (breakLoop==false) ) {

        // calculate score for ngrams in right direction (current ngram is on left border of query)
        if (ngramLocalLeft.isEmpty && !ngramLocalRight.isEmpty) {
            temp = evaluateNGram(ngram = ngramLocalRight.get, maxHits=maxHits,
                               maxLevDistance=maxLevDistance, filter=filter, usePopularity = usePopularity, minScore=minScore, caseInsensitive)

            // if in this direction found score is higher, continue to check for larger ngrams
            if (temp.size> 0 && (maxScoreLocalNgram.size ==0 || temp.head.score > maxScoreLocalNgram.head.score)) {
                maxScoreLocalNgram = temp
                ngramLocalRight = getNgram(maxScoreLocalNgram.head.ngram, terms, termWeights, nNgrams, direction="right")
            }
            // if already found score is the highest, break while
            else breakLoop = true
        }
        // calculate score for ngrams in left direction (current ngram is on right border of query)
        else if (!ngramLocalLeft.isEmpty && ngramLocalRight.isEmpty) {
            temp = evaluateNGram(ngramLocalLeft.get, maxHits=maxHits,
                               maxLevDistance=maxLevDistance, filter=filter, usePopularity = usePopularity, minScore=minScore, caseInsensitive)

            // if in this direction found score is higher, continue to check for larger ngrams
            if (temp.size> 0 && (maxScoreLocalNgram.size ==0 || temp.head.score > maxScoreLocalNgram.head.score)) {
                maxScoreLocalNgram = temp
                ngramLocalLeft = getNgram(maxScoreLocalNgram.head.ngram, terms, termWeights, nNgrams, direction="left")
            }
            // if already found score is the highest, break while
            else breakLoop = true
        }
        // calculate score for ngrams in both directions
        else if (!ngramLocalLeft.isEmpty && !ngramLocalRight.isEmpty) {
            temp = evaluateNGram(ngramLocalLeft.get, maxHits=maxHits,
                               maxLevDistance=maxLevDistance, filter=filter, usePopularity = usePopularity, minScore=minScore, caseInsensitive)
            temp2 = evaluateNGram(ngramLocalRight.get, maxHits=maxHits,
                                maxLevDistance=maxLevDistance, filter=filter, usePopularity = usePopularity, minScore=minScore, caseInsensitive)

            // ngram to left side is higher than current ngram
            if (temp.size> 0 && (maxScoreLocalNgram.size ==0 || temp.head.score > maxScoreLocalNgram.head.score)) {

                // ngram of left side is also higher than right ngram
                if (temp2.size >0 && temp.head.score > temp2.head.score) {
                    maxScoreLocalNgram = temp
                    ngramLocalLeft = getNgram(maxScoreLocalNgram.head.ngram, terms, termWeights, nNgrams, direction="left")
                    ngramLocalRight = None
                }
                // right ngram is higher than left one
                else {
                    maxScoreLocalNgram = temp2
                    ngramLocalLeft = None
                    ngramLocalRight = getNgram(maxScoreLocalNgram.head.ngram, terms, termWeights, nNgrams, direction="right")
                }
            // current ngram is higher than left one
            } else {
                // right ngram is higher than current one
                if (temp2.size > 0 && (maxScoreLocalNgram.size == 0 || temp2.head.score > maxScoreLocalNgram.head.score)) {
                    maxScoreLocalNgram = temp2
                    ngramLocalLeft =  None
                    ngramLocalRight = getNgram(maxScoreLocalNgram.head.ngram, terms, termWeights, nNgrams, direction="right")
                }
                // current ngram is highest -> return
                else breakLoop = true
            }
        }
        // should not occur
        else throw new Exception("THIS SHOULD NOT OCCUR! CHECK NGRAM SEARCH!")
    }

    maxScoreLocalNgram
  }

override def searchDoc(
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
      val nTerms = terms.length
      if( (terms.length > nNgrams) && (nNgrams != -1) ) {
        // calculate score for each ngram and take the N with the highest score
        val maxScoreLocal = terms
          .zipWithIndex
          .sliding(nNgrams)
          .map{arrayOfStrings => 
            termWeights match { 
              case Some(termWeights) => 
                Ngram(arrayOfStrings.map{case (term, index) => term}
                  , arrayOfStrings(0)._2
                  , arrayOfStrings(0)._2+nNgrams
                  , termWeights.slice(arrayOfStrings(0)._2, arrayOfStrings(0)._2+nNgrams)
                )
              case None => 
                Ngram(arrayOfStrings.map{case (term, index) => term}
                  , arrayOfStrings(0)._2
                  , arrayOfStrings(0)._2+nNgrams
                )
            }
          }
          .map(ngram => 
            evaluateNGram(
              ngram=ngram
              , maxHits=maxHits
              , maxLevDistance=maxLevDistance
              , filter=filter
              , usePopularity = usePopularity
              , minScore=minScore, caseInsensitive
          ))
          .filter(m => m.size > 0)
          .toSeq
          .topN(maxHits, (searchMatch1, searchMatch2) => 
            (searchMatch1.size > 0 && (searchMatch2.size == 0 || searchMatch1.head.score < searchMatch2.head.score) ) 
          )
          .toArray

        // For each Ngram with the highest score, check if score can be improved by expanded Ngram
        maxScoreLocal
          .flatMap(maxHit =>  
            searchNgramExpanded(
              maxScoreLocal = maxHit
              , terms = terms
              , termWeights=termWeights
              , nNgrams = nNgrams
              , nTerms = nTerms
              , maxHits = maxHits
              , maxLevDistance= maxLevDistance
              , filter= filter
              , usePopularity= usePopularity
              , minScore = minScore
              , boostAcronyms= boostAcronyms
            )
          )
          .toSeq
          .topN(maxHits, (searchMatch1, searchMatch2) => (searchMatch1.score < searchMatch2.score) )
          .toArray
      } else {
        termWeights match { 
          case Some(termWeights) => 
            evaluateNGram(
              ngram = Ngram(terms, 0, terms.size, termWeights)
              , maxHits=maxHits
              , maxLevDistance=maxLevDistance
              , filter=filter
              , usePopularity = usePopularity
              , minScore = minScore
              , caseInsensitive
            )
          case None => 
            evaluateNGram(
              ngram = Ngram(terms, 0, terms.size)
              , maxHits=maxHits
              , maxLevDistance=maxLevDistance
              , filter=filter
              , usePopularity = usePopularity
              , minScore = minScore
              , caseInsensitive
            )
        }
      }
  }

}
