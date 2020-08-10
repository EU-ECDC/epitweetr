package demy.storage

import demy.util.log
import demy.util.implicits._
import org.apache.http.impl.client.HttpClients
import org.apache.http.client.methods.{HttpPost, HttpGet, HttpPut}
import org.apache.http.entity.mime.MultipartEntityBuilder
import org.apache.http.entity.ContentType
import org.apache.commons.io.IOUtils
import org.apache.commons.codec.binary.Base64

import java.io.{RandomAccessFile, File, FileInputStream, InputStream, StringWriter, SequenceInputStream, ByteArrayInputStream}
import java.nio.file.Files;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel
import java.util.Scanner
import java.security.{MessageDigest, DigestInputStream}
import scala.xml.XML

object EpiFiles {
  def epifileUpload(vooUrl:String, user:String, pwd:String, path:String, name:String,  comment:String) = {
      val endpoint = s"$vooUrl/epifiles/ws"
      val user_encoding = new String(Base64.encodeBase64(s"$user:$pwd".getBytes("UTF-8")),  "UTF-8");
      val file = new RandomAccessFile(path, "r");
      val chunkSize = 1024 * 1024
      val fileSize = file.length 
      val chunks = Math.ceil(1.0*fileSize / chunkSize).toInt
      val file_md5 = LocalFS.computeMD5Hash(path)
      val inChannel = file.getChannel();
      val buffer = ByteBuffer.allocate(chunkSize);
          
      val httpClient = HttpClients.createDefault();
      var procId:Option[String]=None
      
      Range(0, chunks).map{ i =>
          inChannel.read(buffer)
          buffer.flip
          val chunkContent  = new Array[Byte](buffer.remaining())
          buffer.get(chunkContent)
          buffer.clear
          val base64content = new String(Base64.encodeBase64(chunkContent), "UTF-8")
          val md5 = MessageDigest.getInstance("MD5").digest(chunkContent).map(0xFF & _).map { "%02x".format(_) }.foldLeft("") {_ + _}
  
          //println(s"$i .. ${chunkContent.size} $md5 $procId ++ $file_md5 .. ${if(base64content.size > 50) base64content.substring(0, 50) else base64content}")
  
          val uploadChunk = if(i==0) { new HttpPost(s"$endpoint/manifestfile") } 
                            else  { new HttpPost(s"$endpoint/manifestfile/id/${procId.get}")}
          uploadChunk.setHeader("Authorization", "Basic " + user_encoding);
  
          val builder = MultipartEntityBuilder.create()
          builder.addTextBody("content", base64content, ContentType.TEXT_PLAIN)
          builder.addTextBody("hash", md5, ContentType.TEXT_PLAIN);
          builder.addTextBody("total_chunks", chunks.toString(), ContentType.TEXT_PLAIN)
          if(i == 0) {
              builder.addTextBody("filename", name, ContentType.TEXT_PLAIN)
              builder.addTextBody("comment", comment, ContentType.TEXT_PLAIN)
          } else {
              builder.addTextBody("chunk_index", (i+1).toString(), ContentType.TEXT_PLAIN)
              builder.addTextBody("file_hash", file_md5, ContentType.TEXT_PLAIN)
          }
          val multipart = builder.build();
          uploadChunk.setEntity(multipart);
  
          val response = httpClient.execute(uploadChunk)
          val responseEntity = response.getEntity()    
          val content = responseEntity.getContent()
          val writer = new StringWriter();
          IOUtils.copy(content, writer, "UTF-8");
          val resp = writer.toString();
          content.close()

          if(i==0) {
              val xml = XML.loadString(resp)
              val node = xml \\ "process_id"
              procId = Some(node.text)
          }
      }
      inChannel.close()
      file.close()
      procId
  }
  def findFile(commentPattern:Option[String], namePattern:Option[String], vooUrl:String, user:String, pwd:String) = {
      val httpClient = HttpClients.createDefault();
      val endpoint = s"$vooUrl/epifiles/ws"
      val user_encoding = new String(Base64.encodeBase64(s"$user:$pwd".getBytes("UTF-8")),  "UTF-8");
      
      val listFiles = new HttpGet(s"$endpoint/manifest/?version=2")
      listFiles.setHeader("Authorization", "Basic " + user_encoding);

      val listResponse = httpClient.execute(listFiles);
      val listResponseEntity = listResponse.getEntity();    
      val listContent = listResponseEntity.getContent()
      val listWriter = new StringWriter();
      IOUtils.copy(listContent, listWriter, "UTF-8");
      val listResp = listWriter.toString();
      listContent.close()
      val xml = XML.loadString(listResp)
      val ids = (xml \\ "manifest" \ "manifest_files" \ "file").map(x => (x \ "id").map(i => i.text).head)
      val names = (xml \\ "manifest" \ "manifest_files" \ "file").map(x => (x \ "name").map(i => i.text).head)
      val comments = (xml \\ "manifest" \ "comment").map(x => x.text)
      val dates = (xml \\ "manifest" \ "creation_date").map(x => x.text)
      val file = ids.zip(comments).zip(dates.zip(names))
          .map(p => p match {case ((id, comment),(date, name)) => (id, comment, date, name)})
          .sortWith((p1, p2)=> (p1, p2) match {case ((_, _, date1, _), (_, _, date2, _)) => date1 > date2})
          .filter(p => p match {case (_, comment,_ , name) => (
              (namePattern match {case Some(pattern) => !pattern.r.findFirstIn(name).isEmpty case _ => true}) && 
              (commentPattern match {case Some(pattern) => !pattern.r.findFirstIn(comment).isEmpty case _ => true})
              )})
          .take(1)
          .map(p => p match {case (id, comment, date, name) => (name, comment, id, date)})
      file match {
          case Seq(p) => Some(p)
          case _ => None
      }
  }
  def tryDownload(commentPattern:Option[String], namePattern:Option[String], vooUrl:String, user:String, pwd:String) = {
    findFile(commentPattern = commentPattern, namePattern = namePattern, vooUrl = vooUrl, user = user, pwd = pwd) match {
      case Some((name, comment, id, date)) => Some(download(id = id, vooUrl = vooUrl, user = user, pwd = pwd))
      case _ => None
    }
  }
  def download(id:String, vooUrl:String, user:String, pwd:String) = {
      val endpoint = s"$vooUrl/epifiles/ws"
      val user_encoding = new String(Base64.encodeBase64(s"$user:$pwd".getBytes("UTF-8")),  "UTF-8");
      var chunkIndex = 1
      var chunks = 1
      Iterator.continually({
        val loopClient = HttpClients.createDefault();
        if(chunkIndex <= chunks) {
            val getFile = new HttpGet(s"$endpoint/manifestfile/id/$id/chunk_index/$chunkIndex")
            getFile.setHeader("Authorization", "Basic " + user_encoding);
            val getResponse = loopClient.execute(getFile);
            val getResponseEntity = getResponse.getEntity();    
            val getContent = getResponseEntity.getContent()
            val getWriter = new StringWriter();
            IOUtils.copy(getContent, getWriter, "UTF-8");
            val getResp = getWriter.toString();
            val xml = XML.loadString(getResp)
            val filename = (xml \\ "file" \ "filename").text
            val base64 = (xml \\ "file" \ "content").text
            chunks = (xml \\ "file" \ "total_chunks").text.toInt
            val data = Base64.decodeBase64(base64)
            chunkIndex = chunkIndex + 1
            data
        } else null
      }).takeWhile(_ != null)
        .map(bytes => new ByteArrayInputStream(bytes))
        .toJavaEnumeration match { case bytesEnum => new SequenceInputStream(bytesEnum)}
  }
  def exists(id:String, vooUrl:String, user:String, pwd:String) = {
      val httpClient = HttpClients.createDefault();
      val endpoint = s"$vooUrl/epifiles/ws"
      val user_encoding = new String(Base64.encodeBase64(s"$user:$pwd".getBytes("UTF-8")),  "UTF-8");
      val getFile = new HttpGet(s"$endpoint/manifestfile/id/$id/chunk_index/1")
      getFile.setHeader("Authorization", "Basic " + user_encoding);
      val getResponse = httpClient.execute(getFile);
      getResponse.getStatusLine.getStatusCode match {case code => code >=200 && code <300}
  }
  def getLines(linePattern:Option[String], encoding:String = "UTF-8", namePattern:Option[String],commentPattern:Option[String], vooUrl:String, user:String, pwd:String) = {
      val lineRegEx = linePattern match {case Some(s) => Some(s.r) case _ => None}
      tryDownload(namePattern = namePattern, commentPattern = commentPattern, vooUrl = vooUrl, user = user, pwd = pwd) match {
          case Some(byteStream) => {
              val scanner = new Scanner(byteStream, encoding)
              Some(Iterator.continually({
                  if(scanner.hasNextLine) {
                    val nextLine = scanner.nextLine
                    lineRegEx match { 
                        case Some(regex) =>
                            regex.findFirstIn(nextLine) match {
                              case Some(s) => Some(nextLine)
                              case _ => None
                            }
                        case _ => Some(nextLine)
                    }
                  }
                  else null
                 })
                 .takeWhile(_ != null)
                 .flatMap(l => l)
              )
          }
          case _ => None
      }
  }
}
