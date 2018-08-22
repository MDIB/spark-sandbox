package spark_sandbox

import org.apache.spark.sql.functions._
import org.apache.spark.sql._
import spark_sandbox.Implicits._

object DummyTests extends App{


  val spark = {
    SparkSession.builder
      .master("local[*]")
      .appName("Console test")
      .getOrCreate()
  }

  val sq = spark.sqlContext

  import sq.implicits._

  val dummyDf = Seq("bla ble bli","blo blu blimx").toDF("line")

  dummyDf.withColumn("value_splited",explode(split($"line"," "))).show()

  sq.sparkSession.stop()
}
