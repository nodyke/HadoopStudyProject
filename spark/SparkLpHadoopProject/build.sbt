
name := "SparkLpHadoopProject"

version := "0.1"

scalaVersion := "2.10.5"


libraryDependencies += "org.apache.spark" %% "spark-core" % "1.6.3" % "provided"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "1.6.3" % "provided"
libraryDependencies += "org.apache.spark" %% "spark-streaming" % "1.6.3" % "provided"
libraryDependencies += "org.apache.spark" %% "spark-hive" % "1.6.3" % "provided"
libraryDependencies += "org.apache.spark" %% "spark-mllib" % "1.6.3"
libraryDependencies += "joda-time" % "joda-time" % "2.10.1"
libraryDependencies += "org.postgresql" % "postgresql" % "42.2.5.jre7"
libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.1" % Test
libraryDependencies += "javax.servlet" % "javax.servlet-api" % "3.0.1" % Test
libraryDependencies += "com.holdenkarau" %% "spark-testing-base" % "1.6.3_0.10.0" % Test excludeAll(
  ExclusionRule(organization = "org.scalacheck"),
  ExclusionRule(organization = "org.scalactic"),
  ExclusionRule(organization = "org.scalatest")
)
parallelExecution in Test := false
assemblyMergeStrategy in assembly := {
  case "META-INF/services/org.apache.spark.sql.sources.DataSourceRegister" => MergeStrategy.concat
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}




