name := "B"

version := "0.1"

scalaVersion := "2.12.13"


idePackagePrefix := Some("org.snithish")

resolvers += "Sonatype OSS Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots"

// https://mvnrepository.com/artifact/org.apache.spark/spark-core
libraryDependencies += "org.apache.spark" %% "spark-core" % "3.1.0"
// https://mvnrepository.com/artifact/org.apache.spark/spark-sql
libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.1.0"
// https://mvnrepository.com/artifact/org.apache.spark/spark-mllib
libraryDependencies += "org.apache.spark" %% "spark-mllib" % "3.1.0"
// https://mvnrepository.com/artifact/org.plotly-scala/plotly-core
libraryDependencies += "com.github.piotr-kalanski" % "splot" % "0.2.0"
