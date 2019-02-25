name := "intersystems-spark"

organization := "com.intersystems"

scalaVersion := "2.11.8"

// sparkVersion := "2.1.0"

resolvers += Resolver.mavenLocal

libraryDependencies ++= Seq(
	"org.scala-lang" % "scala-library" % scalaVersion.value,
	
	"com.intersystems" % "intersystems-jdbc" % "3.0.0", // resolved from unmanagedBase

	"org.apache.spark" %% "spark-core" % "2.1.0",
	"org.apache.spark" %% "spark-sql" % "2.1.0",
	"org.apache.spark" %% "spark-mllib" % "2.1.0" exclude("org.jpmml","pmml-model"),

	"org.jpmml" % "jpmml-sparkml" % "[1.2.7,2[",

	"com.jsuereth" %% "scala-arm" % "[1.3,)"  // for ManagedResource class
)

unmanagedBase := baseDirectory.value / "../jdbc/target"