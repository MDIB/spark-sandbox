package spark_sandbox

import org.apache.spark.sql.functions._
import org.apache.spark.sql._
import spark_sandbox.Implicits._

import scala.reflect.io.Path

object ShakespeareTfIdf extends App {
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

  val shakespeareDF: DataFrame = sq.read.text("shakespeare.txt")

  val stopWords =
    Set("i", "me", "my", "myself", "we", "our", "ours", "ourselves", "you", "your", "yours", "yourself", "yourselves", "he", "him", "his", "himself", "she", "her", "hers", "herself", "it", "its", "itself", "they", "them", "their", "theirs", "themselves", "THE", "AND",
      "what", "which", "who", "whom", "this", "that", "these", "those", "am", "is", "are", "was", "were", "be", "been", "being", "have", "has", "had", "having", "do", "  does", "did", "doing", "a", "an", "the", "and", "but", "if", "or", "because", "as",
      "until", "while", "of", "at", "by", "for", "with", "about", "against", "between", "into", "through", "during", "before", "after", "above", "below", "to", "from", "up", "down", "in", "out", "on", "off", "over", "under", "again", "further", "then", "once",
      "here", "there", "when", "where", "why", "how", "all", "any", "both", "each", "few", "more", "most", "other", "some", "such", "no", "nor", "not", "only", "own", "same", "so", "than", "too", "very", "s", "t", "can", "will", "just", "don", "should", "now")
      .map(_.toUpperCase)


  val ponctuation = Set(".", ",", ":", ";")

  val replaceW = udf[String, String](s => ponctuation.fold(s)((p, c) => p.replace(c, "")))


  val wordsDF =
    shakespeareDF
      .withColumn("value_splited", explode(split($"value", " ")))
      .select(upper($"value_splited") as "word")


  val cleanDF =
    wordsDF.withColumn("word", replaceW(wordsDF("word")))
      .where(not(trim($"word") isin ((ponctuation union stopWords).toList: _*)))
      .where(not(isnull($"word")))
      .where(trim($"word") =!= "")

  val countsss = cleanDF.count()

  val finalDF =
  cleanDF
    .groupBy($"word")
    .agg(count("word") as "myCount")
    .withColumn("wordFrequency", $"myCount" / countsss)
    .sort($"wordFrequency".desc)
     .withColumn("position",monotonically_increasing_id())
     .select($"position",$"word", $"myCount", $"wordFrequency")
    .repartition(10)

  Path("words").deleteRecursively()

  finalDF.write.csv("words")
  sq.sparkSession.stop()
}

