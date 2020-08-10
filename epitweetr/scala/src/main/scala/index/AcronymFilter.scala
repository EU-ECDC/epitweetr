package demy.mllib.index;

import org.apache.lucene.analysis.CharacterUtils
import org.apache.lucene.analysis.TokenFilter
import org.apache.lucene.analysis.TokenStream
import org.apache.lucene.analysis.LowerCaseFilter
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute
import org.apache.lucene.util.AttributeFactory
import java.io.IOException
import org.apache.lucene.analysis.util.TokenFilterFactory


class AcronymFilterFactory(args:java.util.Map[String,String]) extends TokenFilterFactory(args) {

  // = : return
  override def create(input:TokenStream) = new AcronymFilter(input)
}



// TokenFilter = TokenStream whose input is another TokenStream
// TokenStream = enumerates the sequence of tokens
class AcronymFilter(in:TokenStream) extends TokenFilter(in) {
  // CharTermAttribute = term text of a token
//    val termAtt:CharTermAttribute = addAttribute(new CharTermAttribute());
    val termAtt = addAttribute(classOf[CharTermAttribute])


    @throws(classOf[IOException])
    override def incrementToken(): Boolean = {
        if (input.incrementToken()) {
            var termBuffer = termAtt.buffer()
            val length = termAtt.length

            if (length == 1)
              termAtt.setLength(0)
            else {
              // check if all letters of query are uppercase
              var allLetterUppercase = 
                if (length == 2) Range(0, length).forall(ind => termBuffer(ind).isUpper)
                else false
              // if all uppercase, double the content, for ex. the string "TX" becomes "TXTX"
              if (allLetterUppercase) {
                  termAtt.resizeBuffer(2*length) // grow termAtt double
                  termBuffer = termAtt.buffer()
                  Range(0, length).foreach{i=>
                    termBuffer(length+i)=termBuffer(i)
                  }
                  termAtt.setLength(2*length)
              }
              // if some lowercases, transform all to lowercase
              else {
                  CharacterUtils.toLowerCase(termAtt.buffer(), 0, termAtt.length);
              }
            }
            return true;
            } else
            return false;
    }
}
