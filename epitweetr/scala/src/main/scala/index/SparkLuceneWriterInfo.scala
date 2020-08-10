package demy.mllib.index;

import org.apache.lucene.index.{IndexWriter}
import org.apache.lucene.store.{NIOFSDirectory, FSDirectory, MMapDirectory}
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.apache.lucene.document.{Document, TextField, StringField, IntPoint, BinaryPoint, LongPoint, DoublePoint, FloatPoint, Field, StoredField, DoubleDocValuesField}
import java.io.{ByteArrayOutputStream, ObjectOutputStream}
import demy.storage.{FSNode, WriteMode}
import demy.util.log
case class SparkLuceneWriterInfo(writer:IndexWriter, index:FSDirectory, destination:FSNode){
  lazy val sourceNode = {
    destination.storage.localStorage.getNode(path = index.getDirectory().toString)
  } 
  def indexRow(row:Row, textFieldPosition:Int, popularityPosition:Option[Int]=None, notStoredPositions:Set[Int]=Set[Int](), tokenisation:Boolean=true, caseInsensitive:Boolean=true) {
      val doc = new Document();
      if(tokenisation)  doc.add(new TextField("_text_",row.getAs[String](textFieldPosition) match {case null => null case s if caseInsensitive  =>  s.toLowerCase case s => s}, Field.Store.NO))
      else doc.add(new StringField("_text_",row.getAs[String](textFieldPosition) match {case null => null case s if caseInsensitive  =>  s.toLowerCase case s => s}, Field.Store.NO))
      
      val schema = row.schema.fields.zipWithIndex.foreach(p => p match {case (field, i) => 
        if(!notStoredPositions.contains(i) && !row.isNullAt(i)) field.dataType match {
           case dt:StringType =>   doc.add(new StringField(field.name, row.getAs[String](i), Field.Store.YES))
           case dt:IntegerType => {doc.add(new IntPoint("_point_"+field.name, row.getAs[Int](i)))
                                   doc.add(new StoredField(field.name, row.getAs[Int](i)))}
           case dt:BooleanType => {doc.add(new BinaryPoint("_point_"+field.name, Array(if(row.getAs[Boolean](i)) 1.toByte else 0.toByte )))
                                   doc.add(new StoredField(field.name,  Array(if(row.getAs[Boolean](i)) 1.toByte else 0.toByte )))}
           case dt:LongType => {   doc.add(new LongPoint("_point_"+field.name, row.getAs[Long](i)))
                                   doc.add(new StoredField(field.name, row.getAs[Long](i)))}
           case dt:DoubleType => { doc.add(new DoublePoint("_point_"+field.name, row.getAs[Double](i)))
                                   doc.add(new StoredField(field.name, row.getAs[Double](i)))}
           case dt:FloatType => {  doc.add(new FloatPoint("_point_"+field.name, row.getAs[Float](i)))
                                   doc.add(new StoredField(field.name, row.getAs[Float](i)))
           }
           case _ => { //Storing field as a serialized object
             val serData=new ByteArrayOutputStream();
             val out=new ObjectOutputStream(serData);
             out.writeObject(row.get(i));
             out.close();
             serData.close();
             doc.add(new StoredField(field.name, serData.toByteArray()))
           }
      }})
      popularityPosition match {case Some(i) => doc.add(new DoubleDocValuesField("_pop_", row.getAs[Double](i)))
                                case _ => {} 
      }

      this.writer.addDocument(doc);

  }

    def push(deleteSource:Boolean = false) = {
        val src_str = index.getDirectory().toString
        val source = this.sourceNode 
        //log.msg(s"going to close $index")
        this.writer.commit
        this.writer.close()
        this.index.close()
        if(!this.destination.isLocal) { 
          this.destination.storage.ensurePathExists(path = this.destination.path)
          this.destination.storage.copy(from = source, to = this.destination, writeMode = WriteMode.overwrite )
          if(source.exists) 
            source.delete(recurse = true)
        } else {
          //log.msg("no need to push")
        }

    }
}
