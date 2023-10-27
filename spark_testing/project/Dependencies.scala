import sbt._
object Dependencies {

  val library: Object {
    val sparkSql: ModuleID
    val scalaTest: ModuleID
    val scalaFastTest: ModuleID
    val logger: ModuleID

  } = new {
    object Version {
      lazy val spark = "3.5.0"
    }
    val scalaTest = "org.scalatest" %% "scalatest-flatspec" % "3.2.16" % "test"
    val sparkSql = "org.apache.spark" %% "spark-sql" % Version.spark
    val scalaFastTest = "com.github.mrpowers" % "spark-fast-tests_2.12" % "1.3.0"
    val logger = "org.apache.logging.log4j" %% "log4j-api-scala" % "13.0.0"
  }
}