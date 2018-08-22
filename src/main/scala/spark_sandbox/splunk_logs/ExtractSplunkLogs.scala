package spark_sandbox.splunk_logs

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object ExtractSplunkLogs extends App {
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

  val chargebacksLogsDF = sq.read.text("/home/michel/chargebacks-logs/*")

  def extractEdnAttribute(field: String)(edn: String) = {
    val extractFieldRegex = s".*:$field\\s(.*?)[,}].*".r
    try {
      val extractFieldRegex(fieldValue) = edn
      fieldValue
    } catch {
      case e: MatchError => null
    }
  }

  val ednAttributes = List("log", "line", "cid", "ip", "topic", "path", "method", "status", "user-agent", "client-sub","host")

  val ednAttrUdfTuples = ednAttributes
    .map(attr => (attr, extractEdnAttribute(attr) _))
    .map { case (attr, fn) => (attr, udf(fn)) }

  def withEdnColumn(df: DataFrame): DataFrame =
    df.withColumn("edn", split($"value", " -").getItem(1))

  def withTimestampColumn(df: DataFrame): DataFrame =
    df.withColumn("timestamp", split($"value", " ").getItem(0))

  def withParsedEdnColumns(df: DataFrame):DataFrame =
    ednAttrUdfTuples.foldLeft(df){case (accDF,(attr,parseFn)) =>
     accDF.withColumn(attr,parseFn('edn))
    }


  //logs grouped by log type
  val parsedLogsDF = chargebacksLogsDF
    .transform(withEdnColumn)
    .transform(withParsedEdnColumns)
    .transform(withTimestampColumn)
    .drop("value","edn")

  parsedLogsDF.repartition(5).write.parquet("/home/michel/chargebacks-logs/parsed-logs")


  sq.sparkSession.stop()
}

