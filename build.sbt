ThisBuild / version := "1.0"
ThisBuild / scalaVersion := "2.12.15"
ThisBuild / assembly / mainClass := Some("com.github.aalopatin.SolverMain")

lazy val sparkVersion = "3.2.1"

lazy val root = (project in file("."))
  .settings(
    name := "color-hoop-stack-solver-spark",
    assemblyJarName := "solver.jar",
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-sql" % sparkVersion
    ).map(_ % "provided") ++ Seq(
      "com.typesafe" % "config" % "1.4.2"
    )
  )
