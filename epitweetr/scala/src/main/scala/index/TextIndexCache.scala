package demy.mllib.index;
import org.apache.spark.sql.Row

case class TextIndexCache(query:String, filter:Row, result:Row, on:Long = System.currentTimeMillis) extends Ordered [TextIndexCache] {
  def compare (that: TextIndexCache) = this.on.compare(that.on)
}
