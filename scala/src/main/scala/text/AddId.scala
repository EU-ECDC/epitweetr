package demy.mllib.text

import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.{Param, ParamMap}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Dataset, DataFrame}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{row_number}
import org.apache.spark.ml.util.Identifiable

class AddId(override val uid: String) extends Transformer {
    final val idColumn = new Param[String](this, "idColumn", "The target id Column")
    final val sortColumn = new Param[String](this, "sortColumn", "The order by column")
    def setIdColumn(value: String): this.type = set(idColumn, value)
    def setSortColumn(value: String): this.type = set(sortColumn, value)
    override def transform(dataset: Dataset[_]): DataFrame = dataset.withColumn(get(idColumn).get, row_number().over(Window.orderBy(get(sortColumn).get)))
    override def transformSchema(schema: StructType): StructType = {schema.add(StructField(get(idColumn).get, IntegerType))}
    def copy(extra: ParamMap): AddId = {defaultCopy(extra)}    
    def this() = this(Identifiable.randomUID("AddId"))
}
object AddId {
}
