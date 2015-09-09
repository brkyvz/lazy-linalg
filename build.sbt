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

spShortDescription := "Linear algebra operators for Apache Spark MLlib's linalg package"

spDescription :=
  """It is somewhat cumbersome to write code where you have to convert the MLlib representation of a
    |vector or matrix to Breeze perform the simplest arithmetic operations like addition, subtraction, etc.
    |This package aims to lift that burden, and provide efficient implementations for some of these methods.
    |
    |By keeping operations lazy, this package provides some of the optimizations that you would see
    |in C++ libraries like Armadillo, Eigen, etc.
  """.stripMargin

credentials += Credentials(Path.userHome / ".ivy2" / ".sbtcredentials")
