import Dependencies._

name := "SparkSandbox"

version := "1.0"

scalaVersion := "2.11.8"

val sparkVersion           = "2.3.1"
val clusterDependencyScope = "provided"

resolvers += "Sonatype OSS Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.3.1",
  "org.apache.spark" %% "spark-sql"  % "2.3.1",
  "com.databricks" %% "spark-xml" % "0.4.1"//  "info.bliki.wiki" % "bliki-core" % "3.1.0"
)

mainClass in (Compile, run) := Some("spark_sandbox.shakespeare")


// Statements executed when starting the Scala REPL (sbt's `console` task)
initialCommands := """
import
  project.Functions._,
  project.Processing,
  project.Steps,
  org.apache.spark.sql.SparkSession,
  scala.annotation.{switch, tailrec},
  scala.beans.{BeanProperty, BooleanBeanProperty},
  scala.collection.JavaConverters._,
  scala.collection.{breakOut, mutable},
  scala.concurrent.{Await, ExecutionContext, Future},
  scala.concurrent.ExecutionContext.Implicits.global,
  scala.concurrent.duration._,
  scala.language.experimental.macros,
  scala.math._,
  scala.reflect.macros.blackbox,
  scala.util.{Failure, Random, Success, Try},
  scala.util.control.NonFatal,
  java.io._,
  java.net._,
  java.nio.file._,
  java.time.{Duration => jDuration, _},
  java.lang.System.{currentTimeMillis => now},
  java.lang.System.nanoTime
val sparkNodes = sys.env.getOrElse("SPARK_NODES", "local[*]")
def desugarImpl[T](c: blackbox.Context)(expr: c.Expr[T]): c.Expr[Unit] = {
  import c.universe._, scala.io.AnsiColor.{BOLD, GREEN, RESET}
  val exp = show(expr.tree)
  val typ = expr.actualType.toString takeWhile '('.!=
  println(s"$exp: $BOLD$GREEN$typ$RESET")
  reify { (): Unit }
}
def desugar[T](expr: T): Unit = macro desugarImpl[T]
var _sparkInitialized = false
@transient lazy val spark = {
  _sparkInitialized = true
  SparkSession.builder
    .master(sparkNodes)
    .appName("Console test")
    .getOrCreate()
}
@transient lazy val sc = spark.sparkContext
"""

cleanupCommands in console := """
if (_sparkInitialized) {spark.stop()}
"""


// Do not exit sbt when Ctrl-C is used to stop a running app
cancelable in Global := true

// Improved dependency management
updateOptions := updateOptions.value.withCachedResolution(true)

showSuccess := true
showTiming := true

// Enable colors in Scala console (2.11.4+)
initialize ~= { _ =>
  val ansi = System.getProperty("sbt.log.noformat", "false") != "true"
  if (ansi) System.setProperty("scala.color", "true")
}

// Draw a separator between triggered runs (e.g, ~test)
triggeredMessage := { ws =>
  if (ws.count > 1) {
    val ls = System.lineSeparator * 2
    ls + "#" * 100 + ls
  } else { "" }
}

shellPrompt := { state =>
  import scala.Console.{ BLUE, BOLD, RESET }
  s"$BLUE$BOLD${name.value}$RESET $BOLD\u25b6$RESET "
}
