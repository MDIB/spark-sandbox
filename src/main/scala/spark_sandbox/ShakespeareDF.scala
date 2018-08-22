package spark_sandbox

import org.apache.spark.sql.functions._
import org.apache.spark.sql._
import spark_sandbox.Implicits._

object ShakespeareDF extends  App{

  val spark = {
    SparkSession.builder
      .master("local[*]")
      .appName("Console test")
      .getOrCreate()
  }

  val sq = spark.sqlContext

  val shakespeareDF: DataFrame = sq.read.text("shakespeare.txt")

  val stopWords =
    Set("i", "me", "my", "myself", "we", "our", "ours", "ourselves", "you", "your", "yours", "yourself", "yourselves", "he", "him", "his", "himself", "she", "her", "hers", "herself", "it", "its", "itself", "they", "them", "their", "theirs", "themselves",
      "what", "which", "who", "whom", "this", "that", "these", "those", "am", "is", "are", "was", "were", "be", "been", "being", "have", "has", "had", "having", "do", "  does", "did", "doing", "a", "an", "the", "and", "but", "if", "or", "because", "as",
      "until", "while", "of", "at", "by", "for", "with", "about", "against", "between", "into", "through", "during", "before", "after", "above", "below", "to", "from", "up", "down", "in", "out", "on", "off", "over", "under", "again", "further", "then", "once",
      "here", "there", "when", "where", "why", "how", "all", "any", "both", "each", "few", "more", "most", "other", "some", "such", "no", "nor", "not", "only", "own", "same", "so", "than", "too", "very", "s", "t", "can", "will", "just", "don", "should", "now")
      .map(" "+_+" ")

  val ponctuation = Set(".",",",":",";")

  shakespeareDF
    .where(not($"value".isin(ponctuation.union(stopWords).toList:_*)))
    .withColumn("word",upper($"value"))
    .groupBy($"word")
    .agg(count("word") as "myCount")
    .sort($"myCount".desc)
    .select($"word",$"myCount")
    .limit(100)
    .show()

  sq.sparkSession.stop()
}
