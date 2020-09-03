package demy.mllib.linalg;

case class Coordinate(index:Int, value:Double)
case class SemanticVector(word:String, coord:Vector[Coordinate]) {
    def merge(that:SemanticVector) = {
        val withRepet = (this.coord ++ that.coord).sortWith((c1, c2) => c1.index < c2.index || (c1.index == c2.index && c1.value > c2.value))
        var noRepet = Vector[Coordinate]()
        withRepet.foreach( p => {
            if(noRepet.size == 0 || p.index > noRepet.last.index)
                noRepet = noRepet :+ p
        })
        SemanticVector(this.word, noRepet)
    }
    def sum(that:SemanticVector) = {
        val withRepet = (this.coord ++ that.coord).sortWith((c1, c2) => c1.index < c2.index || (c1.index == c2.index && c1.value > c2.value))
        var noRepet = Vector[Coordinate]()
        withRepet.foreach( p => {
            if(noRepet.size == 0 || p.index > noRepet.last.index)
                noRepet = noRepet :+ p
            else {
                val last = noRepet.last
                noRepet = noRepet.dropRight(1) :+ Coordinate(p.index, p.value + last.value)
            }
        })
        SemanticVector(this.word, noRepet)
    }
    def scale(factor:Double = 1) = {
        if(factor==1.0) this else SemanticVector(this.word, this.coord.map(c => Coordinate(c.index, c.value*factor)))
    }
    def relativeSemantic(reference:SemanticVector) = {
        var i1 = 0
        var i2 = 0
        var newVec = Vector[Coordinate]()
        val maxRef = reference.coord.map(c => Math.abs(c.value)).max
        val maxValue = this.coord.map(c => Math.abs(c.value)).max
        while( i1 < this.coord.size || i2 < reference.coord.size) {
            if( i1 < this.coord.size && i2 < reference.coord.size && this.coord(i1).index==reference.coord(i2).index && this.coord(i1).value!=0) {
                val v = this.coord(i1).value
                val ref = Math.abs(reference.coord(i2).value)
                val tf = 0.5 + 0.5 * (v/maxValue)
                val idf = Math.log(maxRef / ref)
                newVec = newVec :+ Coordinate(this.coord(i1).index, tf*idf)
                i1 = i1 + 1
                i2 = i2 + 1
            } else if ((i1 < this.coord.length && i2 < reference.coord.length && this.coord(i1).index<reference.coord(i2).index) || (i1 < this.coord.length && i2 == reference.coord.length)) {
                i1 = i1 + 1
            } else if ((i1 < this.coord.length && i2 < reference.coord.length && this.coord(i1).index>reference.coord(i2).index) ||  (i1 == this.coord.length && i2 < reference.coord.length)) {
                i2 = i2 + 1
            }
        }
        SemanticVector(this.word, newVec)
    }
    def semanticDiff(docFreq:SemanticVector) = {
        var i1 = 0
        var i2 = 0
        var newVec = Vector[Coordinate]()
        while( i1 < this.coord.size || i2 < docFreq.coord.size) {
            if( i1 < this.coord.size && i2 < docFreq.coord.size && this.coord(i1).index==docFreq.coord(i2).index) {
                val v = this.coord(i1).value
                val c = docFreq.coord(i2).value
                newVec = newVec :+ Coordinate(this.coord(i1).index, v + Math.abs(v)*((v-c)/(Math.abs(v)+Math.abs(c))))
                i1 = i1 + 1
                i2 = i2 + 1
            } else if ((i1 < this.coord.length && i2 < docFreq.coord.length && this.coord(i1).index<docFreq.coord(i2).index) || (i1 < this.coord.length && i2 == docFreq.coord.length)) {
                i1 = i1 + 1
            } else if ((i1 < this.coord.length && i2 < docFreq.coord.length && this.coord(i1).index>docFreq.coord(i2).index) ||  (i1 == this.coord.length && i2 < docFreq.coord.length)) {
                i2 = i2 + 1
            }
        }
        SemanticVector(this.word, newVec)
    }
    def cosineSimilarity(that:SemanticVector) =  {
        var i1 = 0
        var i2 = 0
        var Sv1v2 = 0.0
        var Sv1v1 = 0.0
        var Sv2v2 = 0.0
        while( i1 < this.coord.size || i2 < that.coord.size) {
            if( i1 < this.coord.size && i2 < that.coord.size && this.coord(i1).index==that.coord(i2).index) {
                Sv1v2 = Sv1v2 + this.coord(i1).value*that.coord(i2).value
                Sv1v1 = Sv1v1 + this.coord(i1).value*this.coord(i1).value
                Sv2v2 = Sv2v2 + that.coord(i2).value*that.coord(i2).value
                i1 = i1 + 1
                i2 = i2 + 1
            } else if ((i1 < this.coord.length && i2 < that.coord.length && this.coord(i1).index<that.coord(i2).index) || (i1 < this.coord.length && i2 == that.coord.length)) {
                Sv1v1 = Sv1v1 + this.coord(i1).value*this.coord(i1).value
                i1 = i1 + 1
            } else if ((i1 < this.coord.length && i2 < that.coord.length && this.coord(i1).index>that.coord(i2).index) ||  (i1 == this.coord.length && i2 < that.coord.length)) {
                Sv2v2 = Sv2v2 + that.coord(i2).value*that.coord(i2).value
                i2 = i2 + 1
            }
        }
        Sv1v2 / (Math.sqrt(Sv1v1)*Math.sqrt(Sv2v2))
    }    
    def similarity(that:SemanticVector) = 0.5 + 0.5 * this.cosineSimilarity(that)
    def commonDimSimilarity(that:SemanticVector) =  {
        var i1 = 0
        var i2 = 0
        var sim = 0.0
        while( i1 < this.coord.size || i2 < that.coord.size) {
            if( i1 < this.coord.size && i2 < that.coord.size && this.coord(i1).index==that.coord(i2).index) {
                sim = Math.min(this.coord(i1).value, that.coord(i2).value)
                i1 = i1 + 1
                i2 = i2 + 1
            } else if ((i1 < this.coord.length && i2 < that.coord.length && this.coord(i1).index<that.coord(i2).index) || (i1 < this.coord.length && i2 == that.coord.length)) {
                i1 = i1 + 1
            } else if ((i1 < this.coord.length && i2 < that.coord.length && this.coord(i1).index>that.coord(i2).index) ||  (i1 == this.coord.length && i2 < that.coord.length)) {
                i2 = i2 + 1
            }
        }
        sim
    }
    def getClosestVectorIndex(vectors:Seq[SemanticVector]) = {
        var i = 1
        var maxi = 0
        var maxSim = this.cosineSimilarity(vectors(maxi))
        while(i<vectors.size) {
            val sim = this.cosineSimilarity(vectors(i))
            if(sim>maxSim) {
                maxi = i
                maxSim = sim
            }
            i = i + 1
        }
        maxi
    }
    def size() = {
        this.coord.map(c => c.value * c.value).reduce(_ + _) match {case sum => Math.sqrt(sum)}
    }
    def normalize() = {
        this.scale(1/this.size)
    }
//    def kepTop(top:Int) = SemanticVector(word:ing, coord:Vector[Coordinate])     
    def semanticHash = scala.util.hashing.MurmurHash3.stringHash(this.coord.map(c => c.value).mkString(","))
    def distanceWith(that:SemanticVector) = 1-this.cosineSimilarity(that)
    def relativeSimilarity(that:SemanticVector, reference:SemanticVector) = {
        val maxRef = Math.log(reference.coord.map(c => 1 + Math.abs(c.value)).max)
        var i1 = 0
        var i2 = 0
        var ic = 0
        var Sv1v2 = 0.0
        var Sv1v1 = 0.0
        var Sv2v2 = 0.0
        while( i1 < this.coord.size || i2 < that.coord.size) {
            if( i1 < this.coord.size && i2 < that.coord.size && this.coord(i1).index==that.coord(i2).index) {
                while(ic < reference.coord.size && reference.coord(ic).index < this.coord(i1).index) ic = ic + 1
                if(ic < reference.coord.size  && reference.coord(ic).index == this.coord(i1).index) {
                    val diffWeight = Math.log(1+ Math.abs(reference.coord(ic).value))/maxRef
                    Sv1v2 = Sv1v2 + this.coord(i1).value*diffWeight*that.coord(i2).value*diffWeight
                    Sv1v1 = Sv1v1 + this.coord(i1).value*diffWeight*this.coord(i1).value*diffWeight
                    Sv2v2 = Sv2v2 + that.coord(i2).value*diffWeight*that.coord(i2).value*diffWeight
                }
                i1 = i1 + 1
                i2 = i2 + 1
            } else if ((i1 < this.coord.length && i2 < that.coord.length && this.coord(i1).index<that.coord(i2).index) || (i1 < this.coord.length && i2 == that.coord.length)) {
                while(ic < reference.coord.size  && reference.coord(ic).index < this.coord(i1).index) ic = ic + 1
                if(ic < reference.coord.size && reference.coord(ic).index == this.coord(i1).index) {
                    val diffWeight = Math.log(1 + Math.abs(reference.coord(ic).value))/maxRef
                    Sv1v1 = Sv1v1 + this.coord(i1).value*diffWeight*this.coord(i1).value*diffWeight
                }
                i1 = i1 + 1
            } else if ((i1 < this.coord.length && i2 < that.coord.length && this.coord(i1).index>that.coord(i2).index) ||  (i1 == this.coord.length && i2 < that.coord.length)) {
                while(ic < reference.coord.size && reference.coord(ic).index < that.coord(i2).index) ic = ic + 1
                if(ic < reference.coord.size && reference.coord(ic).index == that.coord(i2).index) {
                    val diffWeight = Math.log(1 + Math.abs(reference.coord(ic).value))/maxRef
                    Sv2v2 = Sv2v2 + that.coord(i2).value*diffWeight*that.coord(i2).value*diffWeight
                }
                i2 = i2 + 1
            }
        }
        Sv1v2 / (Math.sqrt(Sv1v1)*Math.sqrt(Sv2v2))
    }
    def relativeDistanceWith(that:SemanticVector, reference:SemanticVector) = 1-this.relativeSimilarity(that, reference)
    def keepDimensionsOn(that:SemanticVector) = SemanticVector(word = this.word, coord = this.coord.filter(c => that.coord.map(tc => tc.index).contains(c.index))) 
    def toSeq = {
      if(this.coord.size == 0) Seq[Double]()
      else Range(0, this.coord.map(c => c.index).max+1).map(i => this.coord.filter(c => c.index == i) match {case Vector(Coordinate(index, value)) => value case _ =>0.0}).toSeq
    }
    def toVec = {
      if(this.coord.size == 0) Vector[Double]()
      else Range(0, this.coord.map(c => c.index).max+1).map(i => this.coord.filter(c => c.index == i) match {case Vector(Coordinate(index, value)) => value case _ =>0.0}).toVector
    }
}
object SemanticVector{
    def fromWord(word:String) = SemanticVector(word, Vector(Coordinate(Math.abs(scala.util.hashing.MurmurHash3.stringHash(word, scala.util.hashing.MurmurHash3.stringSeed)) % 1048576+300, 1)))
    def fromWordAndValues(word:String, values:Array[Double]) = SemanticVector(word, values.zipWithIndex.map(p => Coordinate(p._2, p._1)).toVector)
    def fromWordAndPairs(word:String, coords:Vector[(Int, Double)]) = SemanticVector(word, coords.map(c => Coordinate(c._1, c._2)))
    
}
