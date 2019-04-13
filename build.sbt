name := "spark-utils"

version := "0.1"

scalaVersion := "2.11.8"

javacOptions := Seq("-source", "1.8", "-target", "1.8")
parallelExecution in Test := false
fork := true

javaOptions ++= Seq("-Xms2G", "-Xmx2G", "-XX:MaxPermSize=2048M", "-XX:+CMSClassUnloadingEnabled")
scalacOptions in (Compile,doc) ++= Seq("-groups", "-implicits")

// publish to sonatype
publishTo := {
  val nexus = "https://oss.sonatype.org/"
  if (isSnapshot.value)
    Some("snapshots" at nexus + "content/repositories/snapshots")
  else
    Some("releases"  at nexus + "service/local/staging/deploy/maven2")
}

licenses := Seq("Apache License 2.0" -> url("http://www.apache.org/licenses/LICENSE-2.0.html"))
homepage := Some(url("https://github.com/kanielc/spark-utils"))

val sparkVersion = "2.3.2"

resolvers ++= Seq(
  Resolver.sonatypeRepo("public")
)

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-mllib" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-hive" % sparkVersion % "provided",
  "org.scalatest" %% "scalatest" % "3.2.0-SNAP10" % Test,
  "org.scalacheck" %% "scalacheck" % "1.13.5" % Test
)