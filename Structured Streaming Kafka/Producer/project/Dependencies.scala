import sbt._
object Dependencies {
  val library: Object {
    val sparkSql: ModuleID
    val sparkStreaming: ModuleID
    val sparkKafka: ModuleID

  } = new {
    object Version {
      lazy val spark = "3.5.0"
    }

    val sparkSql = "org.apache.spark" %% "spark-sql" % Version.spark
    val sparkStreaming = "org.apache.spark" % "spark-streaming_2.13" % Version.spark % "provided"
    val sparkKafka ="org.apache.spark" % "spark-sql-kafka-0-10_2.13" % Version.spark
  }
}