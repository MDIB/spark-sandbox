package spark_sandbox

import org.apache.spark.sql.SQLImplicits

object Implicits  extends  SQLImplicits{
  protected override def _sqlContext = null
}
