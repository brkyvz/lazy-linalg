scalaVersion := "2.10.4"

sparkVersion := "1.5.0"

spName := "brkyvz/lazy-linalg"

version := "0.1.0"

licenses := Seq("Apache-2.0" -> url("http://opensource.org/licenses/Apache-2.0"))

sparkComponents += "mllib"

libraryDependencies += "org.scalatest" %% "scalatest" % "1.9.1" % "test"

libraryDependencies += "holdenk" % "spark-testing-base" % "1.4.1_0.1.1" % "test"

parallelExecution in Test := false

ScoverageSbtPlugin.ScoverageKeys.coverageHighlighting := {
  if (scalaBinaryVersion.value == "2.10") false
  else false
}
