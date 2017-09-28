import scala.util.Properties

import sbt._

val applicationVersion = "1.0-hdp2.6"

organization := "org.cgnal"

name := "spark-tinkerpop"

version := applicationVersion

scalaVersion := "2.11.11"

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
def sparkVersion      = "2.1.0.2.6.0.3-8"
def hadoopVersion     = "2.7.3.2.6.0.3-8"
def hbaseVersion      = "1.1.2.2.6.0.3-8"
def titanVersion      = "1.1.0-hdp2.6.0"
def gremlinVersion    = "3.1.0-incubating"
def jUnitVersion      = "4.8.1"
def scalaTestVersion  = "3.0.0"
def scalaCheckVersion = "1.12.4"

lazy val core = project in here

resolvers in ThisBuild += "hortonworks" at "http://repo.hortonworks.com/content/repositories/releases/"

resolvers in ThisBuild += "hortonworks_groups" at "http://repo.hortonworks.com/content/groups/public/"

resolvers in ThisBuild += "maven-central" at "http://central.maven.org/maven2/"

resolvers in ThisBuild += Resolver.mavenLocal

libraryDependencies += sparkExcludes { "org.apache.spark" %% "spark-core"   % sparkVersion % mainScope }

libraryDependencies += sparkExcludes { "org.apache.spark" %% "spark-graphx" % sparkVersion % mainScope }

libraryDependencies += hadoopClientExcludes { "org.apache.hadoop"    % "hadoop-client"  % hadoopVersion  % mainScope  }

libraryDependencies += hbaseExcludes        { "org.apache.hbase"     % "hbase-common"   % hbaseVersion   % mainScope  }

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


isSnapshot := version.value.endsWith("SNAPSHOT")

publishTo := {
  val nexus = "http://repo.eligotech.com/nexus/"
  if (isSnapshot.value)
    Some("snapshots" at nexus + "content/repositories/snapshots")
  else
    Some("releases" at nexus + "content/repositories/releases")
}

