package demy.mllib.index;

import java.nio.charset.StandardCharsets
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.HashMap

case class Encoding(symbols:String, radix:BigInt,symbolIndex:Map[Char, Int]) {
  val special = ""
  def parse(text:String)  = {
    var value = BigInt(0);
    for (i <- Iterator.range(0, text.size)) {
      this.symbolIndex.get(text.charAt(i)) match {
        case Some(index) =>
          value = value * this.radix + BigInt(index)
        case _ => 
          throw new IllegalArgumentException(s"Not a valid number: $text")
      }
    }
    value
  }
  def toString(value:BigInt) = {
    if (value.signum < 0)
      throw new IllegalArgumentException(s"Negative value not allowed: $value")
    if (value.signum == 0)
      this.symbols.substring(0, 1)
    else {
      val buf = new StringBuilder
      var v = value
      while(v.signum != 0) {
        buf.append(this.symbols.charAt(v.mod(this.radix).intValue()))
        v = v / this.radix
      }
      buf.reverse.toString()
    }
  }
  def convertTo(newEncoding:Encoding, text:String) = {
    newEncoding.toString(parse(text))
  }

  def encode(text:String) = {
    this.toString(Encoding.string2bigint(text))
  }

  def decode(text:String) = {
    Encoding.bigint2string(this.parse(text))
  }
}

object Encoding {
  def apply(symbols:String):Encoding = {
    if(symbols.length() <= 1)
      throw new IllegalArgumentException("Must provide at least 2 symbols: length=" + symbols.length())
    val map = HashMap[Char, Int]()//(symbols.size * 4 / 3 + 1)  
    for(i <- Iterator.range(0, symbols.size)) {
      map.put(symbols.charAt(i), i) match {
        case Some(prevIndex) => throw new IllegalArgumentException(s"Duplicate symbol at index $prevIndex and $i ${symbols.charAt(i)}")
        case _ =>
      }
    }
    Encoding(symbols = symbols, radix =BigInt(symbols.size), symbolIndex = map.toMap)
  }
  def string2bigint(text:String) = {
    var value = BigInt(0)
    text.getBytes(StandardCharsets.UTF_8).foreach(byte => value = value * 255 + (byte & 0xff)) //as unsigned bytes
    value
  }

  def bigint2string(value:BigInt) = {
    val buf = ArrayBuffer[Byte]()
    var v = value
    while(v > 0) {
      buf.append(v.mod(255).toByte)
      v = v / 255
    }
    new String(buf.reverse.toArray, StandardCharsets.UTF_8)
  }

}

