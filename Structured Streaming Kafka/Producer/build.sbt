import Dependencies.library

ThisBuild / scalaVersion := "2.13.12"
ThisBuild / version := "LATEST"

scalacOptions ++= Seq("-target:jvm-11")

lazy val root = (project in file("."))
  .settings(
    name := "producer-streaming",
    libraryDependencies ++= Seq(
      library.sparkSql,
      library.sparkStreaming,
      library.sparkKafka
    ),
    // для собрки толстых JAR через assembly - от ошибок
    assembly / assemblyMergeStrategy := {
      case PathList("META-INF", "MANIFEST.MF") => MergeStrategy.discard
      case _ => MergeStrategy.first
    }
  )