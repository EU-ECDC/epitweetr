package org.ecdc.twitter 

case class GeoElement(
  id:Long
  , name:String
  , alias:String
  , code:String
  , parent_code:String
  , city:String
  , adm4:String
  , adm3:String
  , adm2:String
  , adm1:String
  , country:String
  , adm4_code:String
  , adm3_code:String
  , adm2_code:String
  , adm1_code:String
  , country_code:String
  , population:Int
  , geo_type:String
  , time_zone:String
){
    def first(value:String) = if(value == null || value.length == 0) null else value.split(",")(0)
    def c(value:String, rempl:String) = if(value == null) rempl else value
    def maskTimezone = if(this.time_zone == null) null else this.time_zone.replaceAll("[^\\p{L}]+", ",").split(",").map(s => s"tmzz$s").mkString(",")
}

