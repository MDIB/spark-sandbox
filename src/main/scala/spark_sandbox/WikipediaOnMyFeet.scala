//package spark_sandbox
//
//import java.io.ByteArrayInputStream
//
//import info.bliki.api.PageInfo
//import info.bliki.wiki.dump.InfoBox
//import org.apache.spark.sql.functions._
//import org.apache.spark.sql._
//import spark_sandbox.Implicits._
//
//import scala.collection.mutable.ArrayBuffer
//import scala.io.Source
//import scala.reflect.io.Path
//import scala.xml.pull.{EvElemEnd, EvElemStart, EvText, XMLEventReader}
//
//object WikipediaOnMyFeet extends App {
//  val spark = {
//    SparkSession.builder
//      .master("local[*]")
//      .appName("Console test")
//      .getOrCreate()
//  }
//
//  def nutap[T](v: T): T = {
//    println(v.toString)
//    v
//  }
//
//  val sq = spark.sqlContext
//
//  import sq.implicits._
//
//  val wikiDF = sq.read.format("com.databricks.spark.xml").text("enwikinews-20170820-pages-meta-current.xml")
//
//  //crt c
// def parseXml(inputXmlFileName: String, callback: PageInfoBox => Unit): Unit = {
//    val xml = new XMLEventReader(Source.fromFile(inputXmlFileName))
//    var insidePage = false
//    var buf = ArrayBuffer[String]()
//
//    for (event <- xml) {
//      event match {
//        case EvElemStart(_, "page", _, _) =>
//          insidePage = true
//          val tag = "<page>"
//          buf += tag
//        case EvElemEnd(_, "page") =>
//          val tag = "</page>"
//          buf += tag
//          insidePage = false
//
//          parsePageInfoBox(buf.mkString).foreach(callback)
//          buf.clear
//        case e@EvElemStart(_, tag, _, _) =>
//          if (insidePage) {
//            buf += ("<" + tag + ">")
//          }
//        case e@EvElemEnd(_, tag) =>
//          if (insidePage) {
//            buf += ("</" + tag + ">")
//          }
//        case EvText(t) =>
//          if (insidePage) {
//            buf += t
//          }
//        case _ => // ignore
//      }
//    }
//  }
//
//  def parsePageInfoBox(text: String): Option[PageInfoBox] = {
//    val maybeInfoBox = Option(new WikiPatternMatcher(text).getInfoBox).map(_.dumpRaw())
//
//    maybeInfoBox.flatMap { infoBox =>
//      val wrappedPage = new WrappedPage
//      //The parser occasionally throws exceptions out, we ignore these
//      try {
//        val parser = new WikiXMLParser(
//          new ByteArrayInputStream(text.getBytes),
//          new SetterArticleFilter(wrappedPage))
//        parser.parse()
//      } catch {
//        case e: Exception => //ignore
//      }
//
//      val page = wrappedPage.page
//
//      if (page.getText != null && page.getTitle != null && page.getId != null
//        && page.getRevisionId != null && page.getTimeStamp != null
//        && !page.isCategory && !page.isTemplate) {
//        val pageId = {
//          val textElem = XML.loadString(text)
//          (textElem \ "id").head.child.head.toString
//        }
//        Some(PageInfoBox(pageId, page.getTitle, infoBox))
//      } else {
//        None
//      }
//    }
//  }
//  //ctr v
//
//  sq.sparkSession.stop()
//}
//
