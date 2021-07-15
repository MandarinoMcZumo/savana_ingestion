name := "savana_ingestion"

version := "0.1"

scalaVersion := "2.12.13"

//idePackagePrefix := Some("com.savana.ingestion")

libraryDependencies += "org.apache.spark" % "spark-sql_2.12" % "3.1.1" % "provided"
libraryDependencies += "org.apache.spark" % "spark-hive_2.12" % "3.1.1" % "provided"
libraryDependencies += "com.novocode" % "junit-interface" % "0.11" % "test"
libraryDependencies += "junit" % "junit" % "4.12" % "test"
libraryDependencies += "org.apache.commons" % "commons-compress" % "1.18"
