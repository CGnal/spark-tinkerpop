import scala.util.Properties

import sbt._

val applicationVersion = "1.0-SNAPSHOT"

organization := "org.cgnal"

name := "spark-tinkerpop"

version := applicationVersion

scalaVersion := "2.10.5"

javacOptions in ThisBuild ++= Seq("-source", "1.8", "-target", "1.8", "-Xlint")

def isLibrary = true

def here = file(".")

def tinkerpopExcludes(moduleId: ModuleID) = moduleId
  .exclude("org.apache.hadoop"   , "hadoop-client")
  .exclude("org.apache.hadoop"   , "hadoop-yarn-client")
  .exclude("org.apache.hadoop"   , "hadoop-yarn-api")
  .exclude("org.apache.hadoop"   , "hadoop-yarn-common")
  .exclude("org.apache.hadoop"   , "hadoop-yarn-server-common")
  .exclude("org.apache.tinkerpop", "gremlin-groovy")
  .exclude("org.apache.tinkerpop", "tinkergraph-gremlin")

def titanExcludes(moduleId: ModuleID) = moduleId
  .exclude("org.apache.hadoop"      , "hadoop-client")
  .exclude("org.apache.hadoop"      , "hadoop-yarn-client")
  .exclude("org.apache.hadoop"      , "hadoop-yarn-api")
  .exclude("org.apache.hadoop"      , "hadoop-yarn-common")
  .exclude("org.apache.hadoop"      , "hadoop-yarn-server-common")
  .exclude("org.apache.tinkerpop"   , "gremlin-groovy")
  .exclude("com.thinkaurelius.titan", "titan-cassandra")
  .exclude("com.thinkaurelius.titan", "titan-es")
  .exclude("org.apache.tinkerpop"   , "spark-gremlin")
  .exclude("org.apache.tinkerpop"   , "gremlin-groovy")
  .exclude("org.apache.tinkerpop"   , "tinkergraph-gremlin")

def sparkExcludes(moduleId: ModuleID) = moduleId
  .exclude("org.apache.hadoop", "hadoop-client")
  .exclude("org.apache.hadoop", "hadoop-yarn-client")
  .exclude("org.apache.hadoop", "hadoop-yarn-api")
  .exclude("org.apache.hadoop", "hadoop-yarn-common")
  .exclude("org.apache.hadoop", "hadoop-yarn-server-common")

def hbaseExcludes(moduleId: ModuleID) = moduleId
  .exclude("org.slf4j", "slf4j-log4j12")
  .exclude("javax.servlet", "servlet-api")
  .excludeAll { ExclusionRule(organization = "org.mortbay.jetty") }
  .excludeAll { ExclusionRule(organization = "javax.servlet")     }

def hadoopClientExcludes(moduleId: ModuleID) = moduleId
  .exclude("org.slf4j", "slf4j-api")
  .exclude("javax.servlet", "servlet-api")

def ivyOverride  = ivyScala := ivyScala.value.map { _.copy(overrideScalaVersion = true) }

def mainScope = if (isLibrary) "provided" else "compile"

def scalazVersion     = "7.1.1"
def sparkVersion      = "1.6.0-cdh5.7.0"
def hadoopVersion     = "2.6.0-cdh5.7.0"
def hbaseVersion      = "1.2.0-cdh5.7.0"
def titanVersion      = "1.1.0-cdh5.7.0"
def gremlinVersion    = "3.1.0-incubating"
def jUnitVersion      = "4.8.1"
def scalaTestVersion  = "2.0"
def scalaCheckVersion = "1.12.4"

lazy val core = project in here

resolvers in ThisBuild += "cloudera" at "https://repository.cloudera.com/artifactory/cloudera-repos/"

resolvers in ThisBuild += "maven-central" at "http://central.maven.org/maven2/"

resolvers in ThisBuild += Resolver.mavenLocal

libraryDependencies += sparkExcludes { "org.apache.spark" %% "spark-core"   % sparkVersion % mainScope }

libraryDependencies += sparkExcludes { "org.apache.spark" %% "spark-graphx" % sparkVersion % mainScope }

libraryDependencies += hadoopClientExcludes { "org.apache.hadoop"    % "hadoop-client"  % hadoopVersion  % mainScope withSources() }

libraryDependencies += tinkerpopExcludes    { "org.apache.tinkerpop" % "hadoop-gremlin" % gremlinVersion % mainScope withSources() }

libraryDependencies += tinkerpopExcludes    { "org.apache.tinkerpop" % "spark-gremlin"  % gremlinVersion % mainScope withSources() }

libraryDependencies += hbaseExcludes        { "org.apache.hbase"     % "hbase-common"   % hbaseVersion   % mainScope withSources() }

libraryDependencies += hbaseExcludes        { "org.apache.hbase"     % "hbase-server"   % hbaseVersion   % mainScope withSources() }

libraryDependencies += titanExcludes { "com.thinkaurelius.titan" % "titan-core"   % titanVersion % mainScope }

libraryDependencies += titanExcludes { "com.thinkaurelius.titan" % "titan-hbase"  % titanVersion % mainScope }

libraryDependencies += titanExcludes { "com.thinkaurelius.titan" % "titan-hadoop" % titanVersion % mainScope }

libraryDependencies += "org.apache.tinkerpop" % "gremlin-groovy"      % gremlinVersion % "compile" withSources()

libraryDependencies += "org.apache.tinkerpop" % "tinkergraph-gremlin" % gremlinVersion % "compile" withSources()

libraryDependencies += "org.scalaz"       %% "scalaz-core"   % scalazVersion       % "compile" withSources()

libraryDependencies += "junit"             % "junit"         % jUnitVersion        % "test"

libraryDependencies += "org.scalatest"    %% "scalatest"     % scalaTestVersion    % "test"

libraryDependencies += "org.scalacheck"   %% "scalacheck"    % scalaCheckVersion   % "test"

parallelExecution in Test := false

autoAPIMappings := true

testOptions in Test += Tests.Filter { _ endsWith "Test" }

unmanagedBase := baseDirectory.value / "libext"
