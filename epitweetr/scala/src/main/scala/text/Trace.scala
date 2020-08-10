package demy.mllib.text

import java.sql.Timestamp
import org.apache.spark.sql.functions.{expr}

case class Trace(iteration:Int=0, userStatus:String=null, changed:Boolean=false, score:Double=0.0, changedOn:Timestamp=new Timestamp(System.currentTimeMillis()) , stability:Double=0.0) {
      def setScore(score:Double) = Trace(score = score, userStatus=this.userStatus , changed = this.changed, changedOn = this.changedOn, iteration = this.iteration, stability = this.stability)
      def setChanged(changed:Boolean, iteration:Int) = Trace(score = this.score
                                                          , userStatus=this.userStatus 
                                                          , changed = changed
                                                          , changedOn = if(changed) new Timestamp(System.currentTimeMillis()) else this.changedOn
                                                          , iteration = if(changed) iteration else this.iteration
                                                          , stability = if(changed) this.stability + 1.0 else this.stability - 1.0
                                                          )
      def setUserStatus(userStatus:String, iteration:Int) = Trace(score = this.score
                                                          , userStatus=userStatus 
                                                          , changed = if(userStatus != this.userStatus) true else this.changed
                                                          , changedOn = if(userStatus != this.userStatus) new Timestamp(System.currentTimeMillis()) else this.changedOn
                                                          , iteration = if(userStatus != this.userStatus) iteration else this.iteration
                                                          , stability = if(userStatus != this.userStatus) this.stability + 1.0 else this.stability - 1.0
                                                          )

}

object Trace{
  def fromTuple(tuple:(Int, String, Boolean, Double, Timestamp, Double)) = tuple match {case(iteration, userStatus, changed, score, changedOn, stability) => Trace(iteration=iteration, userStatus=userStatus, changed=changed, score=score, changedOn=changedOn, stability=stability)}
  def traceExp(mapping:Map[String, String]=Map[String, String]()) = 
    expr(s"struct(coalesce(cast(${mapping.getOrElse("iteration", "iteration")} as Int), -1) as iteration, ${mapping.getOrElse("userStatus", "userStatus")} userStatus, coalesce(cast(${mapping.getOrElse("changed", "changed")} as Boolean), false) as changed, coalesce(cast(${mapping.getOrElse("score", "score")} as Double), 0.0) as score, coalesce(cast(${mapping.getOrElse("changedOn", "changedOn")} as Timestamp), '1900-01-01 00:00:00.0') as changedOn, coalesce(cast(${mapping.getOrElse("stability", "stability")} as Double), 0.0) as stability)").as("trace")
}


