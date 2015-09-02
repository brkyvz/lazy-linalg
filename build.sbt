// Your sbt build file. Guides on how to write one can be found at
// http://www.scala-sbt.org/0.13/docs/index.html

scalaVersion := "2.10.4"

sparkVersion := "1.5.0-rc3"

spName := "brkyvz/lazy-linalg"

// Don't forget to set the version
version := "0.1.0"

// All Spark Packages need a license
licenses := Seq("Apache-2.0" -> url("http://opensource.org/licenses/Apache-2.0"))

// Add Spark components this package depends on, e.g, "mllib", ....
sparkComponents += "mllib"

resolvers += "Spark 1.5.0 RC2 Staging" at "https://repository.apache.org/content/repositories/orgapachespark-1143"

libraryDependencies += "org.scalatest" %% "scalatest" % "1.9.1" % "test"

// uncomment and change the value below to change the directory where your zip artifact will be created
// spDistDirectory := target.value

// add any Spark Package dependencies using spDependencies.
libraryDependencies += "holdenk" % "spark-testing-base" % "1.4.1_0.1.1" % "test"

parallelExecution in Test := false

ScoverageSbtPlugin.ScoverageKeys.coverageHighlighting := {
  if (scalaBinaryVersion.value == "2.10") false
  else false
}
