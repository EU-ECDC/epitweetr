package demy.mllib.index;

import demy.mllib.linalg.implicits._
import org.apache.spark.ml.linalg.{Vector => MLVector}

trait VectorIndex {
 def apply(token:Seq[String]):Map[String, MLVector]
}

case class CachedIndex(index:VectorIndex, var cache:Map[String, MLVector]=Map[String, MLVector]()) extends VectorIndex {
  def apply(token:Seq[String]) = {
    val fromIndex = token.filter(t => !cache.contains(t))
    if(fromIndex.size > 0)
      cache ++ index(fromIndex)
    else 
      cache
  }
  def setCache(tokens:Seq[String]):this.type = {
    cache = index(tokens)
    this
  }
}


case class MapIndex(cache:Map[String, MLVector]) extends VectorIndex {
  def apply(token:Seq[String]) = {
    token.map(t => (t, cache(t))).toMap
  }
}
