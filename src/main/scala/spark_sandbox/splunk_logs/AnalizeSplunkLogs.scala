package spark_sandbox.splunk_logs

import org.apache.spark.sql.functions._
import org.apache.spark.sql._
import spark_sandbox.Implicits._

import scala.reflect.io.Path

object AnalizeSplunkLogs extends App {
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

  val baseFolder = "/home/michel/spark-data/parsed-splunk-logs"
  val cbLogsDF = sq.read.parquet(baseFolder)

  def cidCountAnalisys() {
    cbLogsDF.groupBy("cid").count().orderBy($"count".desc).limit(1000).repartition(1).write.csv(baseFolder + "/biggest-cids")
  }

  def httpAnalisys(): Unit ={
    cbLogsDF.groupBy("method","path").count().orderBy($"count".desc).repartition(1).write.csv(baseFolder + "/http-analisys")
  }

  cidCountAnalisys()

  sq.sparkSession.stop()
}

