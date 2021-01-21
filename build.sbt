name := "hive"

version := "0.1"

scalaVersion := "2.11.8"

libraryDependencies += "org.apache.hive" % "hive-jdbc" % "1.1.0-cdh5.16.2"

resolvers += "Cloudera" at "https://repository.cloudera.com/artifactory/cloudera-repos/"