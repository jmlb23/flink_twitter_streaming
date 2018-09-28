import sbt._

object Dependencies {
  lazy val flink = "org.scalatest" %% "scalatest" % "3.0.5" ::
    "org.apache.flink" %% "flink-scala" % "1.6.1" ::
    "org.apache.flink" %% "flink-streaming-scala" % "1.6.1" ::
    "org.apache.flink" %% "flink-connector-twitter" % "1.6.1" ::
    "commons-logging" % "commons-logging" % "1.2"::
    "io.argonaut" %% "argonaut" % "6.2.1"::
    Nil

}
