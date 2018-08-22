package spark_sandbox

import org.apache.spark.sql.functions._
import org.apache.spark.sql._
import spark_sandbox.DummyTests.sq
import spark_sandbox.Implicits._

object DataHelp extends App {
  val spark = {
    SparkSession.builder
      .master("local[*]")
      .appName("Console test")
      .getOrCreate()
  }

  def nutap[T](v: T): T = {
    println(v.toString)
    v
  }

  val sq = spark.sqlContext
  import sq.implicits._

  val sc = Seq("2018-01-01").toDF
  val datesList = Seq("2018-07-01", "2018-07-02")
  val dfList = datesList.map(identity).toDF
  sc.union(dfList).show

  sq.sparkSession.stop()
}
