ThisBuild / scalaVersion := "2.12.18"
ThisBuild / organization := "com.day27"
ThisBuild / version := "1.0"

lazy val root = (project in file("."))
  .settings(
    name := "Day27-Real-Time-Banking-Project",
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % "3.5.1",
      "org.apache.spark" %% "spark-streaming" % "3.5.1"
    ),
    Compile / run / fork := true,
    Compile / run / javaOptions ++= Seq(
      "-Xms512m",
      "-Xmx1g",
      "--add-opens=java.base/java.lang=ALL-UNNAMED",
      "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED",
      "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED",
      "--add-opens=java.base/java.io=ALL-UNNAMED",
      "--add-opens=java.base/java.nio=ALL-UNNAMED",
      "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
      "--add-opens=java.base/java.util=ALL-UNNAMED",
      "--add-opens=java.base/sun.util.calendar=ALL-UNNAMED"
      )
  )
