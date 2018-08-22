package spark_sandbox

import org.apache.spark.sql.functions._
import org.apache.spark.sql._
import spark_sandbox.Implicits._

import scala.reflect.io.Path

object PercapitaMun extends App {
  val spark = {
    SparkSession.builder
      .master("local[*]")
      .appName("Console test")
      .getOrCreate()
  }

  def nuTap[T](v: T): T = {
    println(v)
    v
  }

  val sq = spark.sqlContext

  import sq.implicits._

  val perCapitaDf = sq.read
    .option("header", "true")
    .option("encoding", "ISO-8859-1")
    .csv("vw_pib_percapita.csv")

  def withDoublePIB(df: DataFrame) =
    df.withColumn("PIB", df("PIB").cast("Double"))

  println("menor")
  perCapitaDf.drop("geom").transform(withDoublePIB).orderBy($"PIB").show(1)
  println("maior")
  perCapitaDf.drop("geom").transform(withDoublePIB).orderBy($"PIB".desc).show(1)

  perCapitaDf.rdd.partitions.foreach(println)

  println(perCapitaDf.rdd.map(_.get(5).asInstanceOf[String].toDouble).reduce(_ + _)/perCapitaDf.count())

  perCapitaDf
    .drop("geom", "Descrição")
    .where($"classe" === "3")
    .orderBy($"PIB".desc)
    .show(10)

  sq.sparkSession.stop()
}
