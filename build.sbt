name := "Project_2_solution"

version := "0.1.0"
//"2.11.8"
//"2.12.15"
scalaVersion := "2.11.8"
val sparkVersion = "2.3.1"

libraryDependencies += "org.apache.spark" %% "spark-core" % sparkVersion

libraryDependencies += "org.apache.spark" %% "spark-sql" % sparkVersion

libraryDependencies +=  "org.scalaj" %% "scalaj-http" % "2.3.0"
//
libraryDependencies ++= Seq("org.slf4j" % "slf4j-api" % "1.7.13",
                            "org.slf4j" % "slf4j-log4j12" % "1.7.13")

libraryDependencies += "org.scalactic" %% "scalactic" % "3.1.1"

libraryDependencies += "org.scalatest" %% "scalatest" % "3.1.1" % Test

libraryDependencies ++= Seq(
  "junit" % "junit" % "4.8.1" % "test"
)
libraryDependencies += "com.github.nscala-time" %% "nscala-time" % "2.30.0"

libraryDependencies += "com.github.tototoshi" %% "scala-csv" % "1.3.10"

//libraryDependencies += "com.storm-enroute" %% "scalameter-core" % "0.6"

//LP solver
//libraryDependencies ++= Seq(
//  "com.github.vagmcs" %% "optimus" % "3.4.1",
//  "com.github.vagmcs" %% "optimus-solver-oj" % "3.4.1",
//  "com.github.vagmcs" %% "optimus-solver-lp" % "3.4.1",
//"com.github.vagmcs" %% "optimus-solver-gurobi" % "3.4.1",
//"com.github.vagmcs" %% "optimus-solver-mosek" % "3.4.1"
//)

//Interpolation
//libraryDependencies  ++= Seq(
//  "org.scalanlp" %% "breeze" % "2.0.1-RC1",
//  "org.scalanlp" %% "breeze-viz" % "2.0.1-RC1"
//)

//libraryDependencies ++= Seq(
//  // One SLF4J implementation (log4j-slf4j-impl) is here:
//  "org.apache.logging.log4j" % "log4j-api" % "2.6.1",
//  "org.apache.logging.log4j" % "log4j-core" % "2.6.1",
//  "org.apache.logging.log4j" % "log4j-slf4j-impl" % "2.6.1",
//  // The other implementation (slf4j-log4j12) would be transitively
//  // included by Spark. Prevent that with exclude().
//  "org.apache.spark" %% "spark-core" % sparkVersion exclude("org.slf4j", "slf4j-log4j12")
//)

assemblyMergeStrategy in assembly := {
 case PathList("META-INF", xs @ _*) => MergeStrategy.discard
 case x => MergeStrategy.first
}