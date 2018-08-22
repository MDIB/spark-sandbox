package spark_sandbox

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql._

object shakespeare  extends  App {


  val spark = {
    SparkSession.builder
      .master("local[*]")
      .appName("Console test")
      .getOrCreate()
  }
  val sc = spark.sparkContext

  val shakespeareRDD = sc.textFile("shakespeare.txt")

  val stopWords =
    Set("i", "me", "my", "myself", "we", "our", "ours", "ourselves", "you", "your", "yours", "yourself", "yourselves", "he", "him", "his", "himself", "she", "her", "hers", "herself", "it", "its", "itself", "they", "them", "their", "theirs", "themselves",
        "what", "which", "who", "whom", "this", "that", "these", "those", "am", "is", "are", "was", "were", "be", "been", "being", "have", "has", "had", "having", "do", "  does", "did", "doing", "a", "an", "the", "and", "but", "if", "or", "because", "as",
        "until", "while", "of", "at", "by", "for", "with", "about", "against", "between", "into", "through", "during", "before", "after", "above", "below", "to", "from", "up", "down", "in", "out", "on", "off", "over", "under", "again", "further", "then", "once",
        "here", "there", "when", "where", "why", "how", "all", "any", "both", "each", "few", "more", "most", "other", "some", "such", "no", "nor", "not", "only", "own", "same", "so", "than", "too", "very", "s", "t", "can", "will", "just", "don", "should", "now")

  def replaceW(rp: Set[String])(s :String) =
    rp.fold(s)((p,c) => p.replace(c," "))

  val replaceStopWords  = replaceW(stopWords.map(" "+_+" "))_

  val ponctuation = Set(".",",",":",";")

  val replacePonctuation = replaceW(ponctuation)_

  val wordsCountRDD: RDD[(String, Int)] = shakespeareRDD.map(_.toLowerCase).filter(x=> !x.isEmpty).map(replaceStopWords).map(replacePonctuation).flatMap(_.split(" ")).groupBy(identity).mapValues(_.size)


  println("basic world count stuff")
  wordsCountRDD.sortBy(_._2, false).filter(_._1.contains("me")).filter(_._1.contains("think")).collect().foreach(println)
  wordsCountRDD.sortBy(_._2, false).take(100).foreach(println)

  sc.stop()
}
