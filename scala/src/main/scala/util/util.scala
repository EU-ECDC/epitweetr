package demy.util
import org.apache.spark.sql.{Dataset, DataFrame, Encoder, SparkSession}
import org.apache.spark.sql.functions.{col}
import scala.util.Try
import scala.reflect.runtime.universe.TypeTag

object implicits {
  implicit class IterableUtil[T](val elems: Iterable[T]) {
    def topN(n:Int, smallest:(T, T) => Boolean) = {
        elems.toSeq.sortWith((a, b) => smallest(b, a)).take(n)
    }
  }

  implicit class IteratorToJava[T](it: Iterator[T]) {
    def toJavaEnumeration = EnumerationFromIterator(it)
  }
}

case class MergedIterator[T, U](a:Iterator[T], b:Iterator[U], defA:T, defB:U) extends Iterator[(T, U)] {
  def hasNext = a.hasNext || b.hasNext
  def next = (if(a.hasNext) a.next else defA, if(b.hasNext) b.next else defB)
}


case class EnumerationFromIterator[T](it:Iterator[T]) extends java.util.Enumeration[T] {
  def hasMoreElements() = it.hasNext
  def nextElement() = it.next()
}




object util {
    def decodeColumnName(s:String):String = {
      val toEnc = Seq(',',';','{','}','(',')','\n','\t','=', ' ').map(c => (c.toString, s">>${c.toInt}<<"))
      (s.indexOf(">>"), s.indexOf("<<")) match {
        case (i0, i1) if i0 >=0 && i1 > i0 => 
          decodeColumnName(s.slice(0, i0) + s.slice(i0+2, i1).toInt.toChar.toString + s.substring(i1+2))
        case _ => 
          s
      }
    }
    def restoreCheckPoint(path:String)(implicit spark:SparkSession) = {
       
      Try(spark.read.parquet(path))
        .toOption
        .map(df => df.select(
          df.schema
            .map(f => 
              (f.name, decodeColumnName(f.name)) match {
                case (name, decoded) if name != decoded => col(name).as(decoded)
                case _ => col(f.name)  
              } 
            )
           :_* )
        )
    }
    def checkpoint[T : Encoder : TypeTag] (ds:Dataset[T], path:String):Dataset[T] = checkpoint(ds, path, None, false)
    def checkpoint[T : Encoder : TypeTag] (ds:Dataset[T], path:String, reuseExisting:Boolean):Dataset[T] = checkpoint(ds, path, None, reuseExisting)
    def checkpoint[T : Encoder : TypeTag] (ds:Dataset[T], path:String, partitionBy:Option[Array[String]]):Dataset[T] = checkpoint(ds, path, partitionBy, false)
    def checkpoint[T : Encoder : TypeTag] (ds:Dataset[T], path:String, partitionBy:Option[Array[String]], reuseExisting:Boolean):Dataset[T] =
    {  
      if((new java.io.File(path)) match {case f => !reuseExisting || !f.exists && f.listFiles().isEmpty})
        ((ds.write.mode("overwrite"), partitionBy)  match {
          case(w, Some(cols)) => w.partitionBy(cols:_*)
          case(w, _) => w
        }).parquet(path)

      ds.sparkSession.read.parquet(path).as[T]
    }

    def checkpoint(df:DataFrame, path:String, partitionBy:Option[Array[String]]=None, reuseExisting:Boolean = false):DataFrame =
    {
      val toEnc = Seq(',',';','{','}','(',')','\n','\t','=', ' ').map(c => (c.toString, s">>${c.toInt}<<"))

      val encCols = df.schema.flatMap{f => 
          val none = None.asInstanceOf[Option[String]]
          toEnc
            .filter(c => f.name.contains(c._1))
            .foldLeft(none)((current, iter) => current.orElse(Some(f.name)).map(newCol => newCol.replace(iter._1, iter._2))).map(newCol => (f.name, newCol))
      }

      val encodedDF = encCols.foldLeft(df)((current, iter) => current.withColumnRenamed(iter._1, iter._2))
      if((new java.io.File(path)) match {case f => !reuseExisting || !f.exists || f.listFiles().isEmpty})
        ((encodedDF.write.mode("overwrite"), partitionBy)  match {
          case(w, Some(cols)) => w.partitionBy(cols:_*)
          case(w, _) => w
        }).parquet(path)
      val readDF = df.sparkSession.read.parquet(path)

      encCols.foldLeft(readDF)((current, iter) => current.withColumnRenamed(iter._2, iter._1))
    }
    def getStackTraceString(e:Exception) = {
     val sw = new java.io.StringWriter();
     val pw = new java.io.PrintWriter(sw, true)
     e.printStackTrace(pw)
     sw.getBuffer().toString()
    }
    
    def getStackTrace(e:Exception) = {
      val sw = new java.io.StringWriter();
      val pw = new java.io.PrintWriter(sw, true)
      e.printStackTrace(pw)
      sw.getBuffer().toString()
    }
}
