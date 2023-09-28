import Dependencies.library
ThisBuild / organization  := "test"
ThisBuild / scalaVersion := "2.12.18"
ThisBuild / version := "LATEST"

scalacOptions ++= Seq("-target:jvm-11")

lazy val root = (project in file("."))
  .settings(
    name := "test-project",
    libraryDependencies ++= Seq(
      library.sparkSql
    ),
    // для собрки толстых JAR через assembly - от ошибок
    assembly / assemblyMergeStrategy := {
      case PathList("META-INF", "MANIFEST.MF") => MergeStrategy.discard
      case _ => MergeStrategy.first
    }
  )