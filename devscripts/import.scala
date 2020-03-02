spark.read.options(Map("header" ->"true", "escape"->"\"")).csv("/home/fod/deleteme/topics.csv").select("Keywords", "Negative", "DP").as[(String, String, String)].map{case (kw, neg, topic) => ((if(kw == null) Seq[String]() else kw.split("OR").map(_.trim).toSeq)++(if(neg == null) Seq[String]() else neg.split("OR").map(_.trim).map(n => s"-$n").toSeq),if(topic == null) "TBD" else topic) }.toDF("query", "topic").where(size(col("query"))>0).distinct.coalesce(1).write.option("lineSep", ",\n").mode("overwrite").json("/home/fod/deleteme/topics.json")
