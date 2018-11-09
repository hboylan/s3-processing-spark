name := "unseenstars-spark"

// app version
version := "1.0"

// scala version
scalaVersion := "2.11.12"

// sbt-spark-package
val spVersion = "2.3.2"
sparkVersion := spVersion
sparkComponents ++= Seq("sql")
spIgnoreProvided := true

// additional repositories
resolvers += "spark-packages" at "https://dl.bintray.com/spark-packages/maven/"
resolvers += "mulesoft" at "https://repository.mulesoft.org/nexus/content/repositories/public/"

// external dependencies
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % spVersion % "provided",
  "com.amazon.redshift" % "redshift-jdbc42-no-awssdk" % "1.2.16.1027",
  "com.databricks" %% "spark-csv" % "1.5.0",
  "com.databricks" %% "spark-redshift" % "3.0.0-preview1",
  "net.snowflake" %% "spark-snowflake" % "2.4.9",
  "net.snowflake" % "snowflake-jdbc" % "3.6.16"
)

// include dependencies at runtime
run in Compile := Defaults.runTask(fullClasspath in Compile, mainClass in(Compile, run), runner in(Compile, run)).evaluated
runMain in Compile := Defaults.runMainTask(fullClasspath in Compile, runner in(Compile, run)).evaluated

// assemble JAR
assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}