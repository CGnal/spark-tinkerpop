import sbt.Keys._
import sbt._

scalaVersion := "2.11.11"

def here = file(".")

def hadoopHome = s"-Dhadoop.home.dir=${sys.props.getOrElse("hadoop.home.dir", "not-found")}"

lazy val root = Project(id = "root", base = here).aggregate(core, examples)

lazy val core = Project(id = "core", base = file("core"))

lazy val examples = Project(id = "examples", base = file("examples")).settings(
  javaOptions in run     += hadoopHome,
  javaOptions in console += hadoopHome,
  aggregate in update    := false
) dependsOn core