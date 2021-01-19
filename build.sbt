import sbt._
import Keys._

scalacOptions ++= Seq(
  "-feature",
  "-deprecation",
  "-unchecked",
  "-language:postfixOps",
  "-language:higherKinds",
  "-Ypartial-unification")

fork := true

lazy val commonSettings = Seq(
  organization := "com.spotify",
  version := "1.0.0"
)

lazy val root: Project = project
  .in(file("."))
  .settings(commonSettings)
  .settings(name := "scio-cats-effect-root")
  .aggregate(`scio-cats-effect`)

lazy val `scio-cats-effect`: Project = project
  .in(file("scio-cats-effect"))
  .settings(commonSettings)
  .settings(
    name := "scio-cats-effect",
    description := "Scio Cats Effect",
    libraryDependencies ++= Seq(
      "org.typelevel" %% "cats-effect" % "2.3.1" withSources() withJavadoc(),
      "org.typelevel" %% "cats-effect-laws" % "2.3.1" % "test",
      "org.slf4j" % "slf4j-api" % "1.7.5",
      "org.slf4j" % "slf4j-simple" % "1.7.5",
      //"com.google.guava" % "guava" % "28.2-jre",
      "com.spotify" %% "scio-core" % "0.9.6",
      "com.spotify" %% "scio-test" % "0.9.6" % "test",
      "org.apache.beam" % "beam-runners-direct-java" % "2.24.0",
      //"org.apache.beam" % "beam-runners-google-cloud-dataflow-java" % "2.24.0"
    )
  )
