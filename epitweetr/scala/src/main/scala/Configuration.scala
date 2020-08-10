package demy

import demy.storage.{Storage, FSNode}

case class Configuration(values:Map[String, String], defaultValues:Map[String, String]) {
  def get(prop:String) = values.get(prop)
  def getOrDefault(prop:String) = values.getOrElse(prop, defaultValues.get(prop) match {case Some(v) => v case _ => throw new Exception(s"Cannot found value or default for property $prop")})

}

object Configuration {

  def apply(defaultValues:Map[String, String]):Configuration = Configuration(values = defaultValues, defaultValues = defaultValues)
  def apply(valuesNode:Option[FSNode], defaultValues:Map[String, String]):Configuration = {
    valuesNode match {
      case Some(node) =>
        val values = 
          node.getContentAsJson.getOrElse(new Exception(s"Cannot load configuration file ${node.path} as json")) match {
            case seq:Seq[_] => { 
              seq.flatMap(elem => elem match { 
                  case props:Map[_,_] => props.toSeq.map{case (p, v) => (p.toString, v.toString)}
                  case _ => throw new Exception(s"cannot interpret configuratin file ${node.path}")
                }
              ).toMap
            }
            case props:Map[_,_] => props.map{case(p, v) => (p.toString, v.toString)}  
            case e:Exception => throw e
            case _ => throw new Exception(s"We found a problem loading ${node.path}")
          }
        Configuration(values = values, defaultValues = defaultValues)
      case _ => Configuration(defaultValues = defaultValues)
    }

  }
}

