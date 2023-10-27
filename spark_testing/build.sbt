import Dependencies.library

ThisBuild / scalaVersion := "2.12.6"
ThisBuild / version := "LATEST"

lazy val sparkVersion = "3.5.0"
lazy val root = (project in file("."))
  .settings(
    name := "test-project",
    libraryDependencies ++= Seq(
      library.sparkSql,
      library.scalaTest,
      library.scalaFastTest,
      library.logger
    ),
    // для собрки толстых JAR через assembly - от ошибок
    assembly / assemblyMergeStrategy := {
      case PathList("META-INF", "MANIFEST.MF") => MergeStrategy.discard
      case _ => MergeStrategy.first
    }
  )