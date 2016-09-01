import sbt._

import sbtassembly.MergeStrategy

import com.typesafe.sbt.packager.archetypes.JavaAppPackaging
import com.typesafe.sbt.packager.universal.UniversalPlugin

val applicationVersion = "1.0-SNAPSHOT"

organization := "org.cgnal"

name := "spark-tinkerpop-examples"

version := applicationVersion

scalaVersion := "2.10.5"

def assemblyName = "spark-tinkerpop-examples"

def isLibrary = false

def here = file(".")
def assemblyDir = file("assembly")

def named(file: File, relativeTo: String = "") = file -> s"$relativeTo${file.getName}"
def jarName(scalaBinary: String, jarVersion: String) = s"graph_$scalaBinary-$jarVersion.jar"

def thinJar(scalaBinary: String, jarVersion: String) = file(s"${sys.props("user.dir")}/target/scala-$scalaBinary/${jarName(scalaBinary, jarVersion)}")
def fatJar(scalaBinary: String, jarVersion: String)  = file(s"${sys.props("user.dir")}/assembly/target/scala-$scalaBinary/$assemblyName-$jarVersion.jar")

def tinkerpopExcludes(moduleId: ModuleID) = moduleId
  .exclude("org.apache.hadoop"   , "hadoop-client")
  .exclude("org.apache.hadoop"   , "hadoop-yarn-client")
  .exclude("org.apache.hadoop"   , "hadoop-yarn-api")
  .exclude("org.apache.hadoop"   , "hadoop-yarn-common")
  .exclude("org.apache.hadoop"   , "hadoop-yarn-server-common")
  .exclude("org.apache.tinkerpop", "gremlin-groovy")

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

// Assembly settings
def assemblyResolutionStrategy = assemblyMergeStrategy in assembly <<= (assemblyMergeStrategy in assembly) { assemblyStrategy }

def assemblyNoCache            = assemblyOption in assembly := (assemblyOption in assembly).value.copy(cacheUnzip = false)

def assemblyJar                = assemblyJarName in assembly := s"$assemblyName-$applicationVersion.jar"

def assemblyStrategy(currentStrategy: String => MergeStrategy): String => MergeStrategy = {
  case s if s endsWith "spark/unused/UnusedStubClass.class" => MergeStrategy.rename
  case s if s contains "META-INF"                           => MergeStrategy.discard
  case other                                                => MergeStrategy.first
}

def coreVersion       = "1.0-SNAPSHOT"
def scallopVersion    = "2.0.1"
def scalazVersion     = "7.1.1"
def sparkVersion      = "1.6.0-cdh5.7.0"
def hadoopVersion     = "2.6.0-cdh5.7.0"
def hbaseVersion      = "1.2.0-cdh5.7.0"
def titanVersion      = "1.1.0-cdh5.7.0"
def gremlinVersion    = "3.2.0-incubating"
def jUnitVersion      = "4.8.1"
def scalaTestVersion  = "2.0"
def scalaCheckVersion = "1.12.4"

lazy val root = project in here enablePlugins JavaAppPackaging enablePlugins UniversalPlugin

lazy val projectAssembly = { project in assemblyDir }.settings(ivyOverride, assemblyJar, assemblyResolutionStrategy, assemblyNoCache) dependsOn root

resolvers in ThisBuild += "cloudera" at "https://repository.cloudera.com/artifactory/cloudera-repos/"

resolvers in ThisBuild += "maven-central" at "http://central.maven.org/maven2/"

resolvers in ThisBuild += Resolver.mavenLocal

libraryDependencies += sparkExcludes { "org.apache.spark" %% "spark-core"   % sparkVersion % mainScope }

libraryDependencies += sparkExcludes { "org.apache.spark" %% "spark-sql"    % sparkVersion % mainScope }

libraryDependencies += sparkExcludes { "org.apache.spark" %% "spark-graphx" % sparkVersion % mainScope }

libraryDependencies += hadoopClientExcludes { "org.apache.hadoop"    % "hadoop-client"  % hadoopVersion  % mainScope withSources() }

libraryDependencies += tinkerpopExcludes    { "org.apache.tinkerpop" % "hadoop-gremlin" % gremlinVersion % mainScope withSources() }

libraryDependencies += tinkerpopExcludes    { "org.apache.tinkerpop" % "spark-gremlin"  % gremlinVersion % mainScope withSources() }

libraryDependencies += hbaseExcludes        { "org.apache.hbase"     % "hbase-common"   % hbaseVersion   % mainScope withSources() }

libraryDependencies += hbaseExcludes        { "org.apache.hbase"     % "hbase-server"   % hbaseVersion   % mainScope withSources() }

libraryDependencies += "com.thinkaurelius.titan" % "titan-core"   % titanVersion % mainScope

libraryDependencies += "com.thinkaurelius.titan" % "titan-hbase"  % titanVersion % mainScope

libraryDependencies += "com.thinkaurelius.titan" % "titan-hadoop" % titanVersion % mainScope

libraryDependencies += "org.cgnal"        %% "spark-tinkerpop"    % coreVersion  % mainScope

libraryDependencies += "org.rogach"       %% "scallop"       % scallopVersion      % "compile" withSources()

libraryDependencies += "org.scalaz"       %% "scalaz-core"   % scalazVersion       % "compile" withSources()

libraryDependencies += "junit"             % "junit"         % jUnitVersion        % "test"

libraryDependencies += "org.scalatest"    %% "scalatest"     % scalaTestVersion    % "test"

libraryDependencies += "org.scalacheck"   %% "scalacheck"    % scalaCheckVersion   % "test"

parallelExecution in Test := false

fork in run := true

outputStrategy := Some { StdoutOutput }

autoAPIMappings := true

testOptions in Test += Tests.Filter { _ endsWith "Spec" }

unmanagedBase := baseDirectory.value / "libext"
