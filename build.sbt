name := "scalaraft"

version := "1.0"

scalaVersion := "2.10.2"


libraryDependencies ++= Seq(
  "com.typesafe.akka" %% "akka-actor" % "2.2.0",
  "com.typesafe.akka" %% "akka-testkit" % "2.2.0",
  "com.typesafe.akka" %% "akka-remote" % "2.2.0",
  "org.scalatest" %% "scalatest" % "2.0" % "test",
  "junit" % "junit" % "4.11" % "test",
  "com.novocode" % "junit-interface" % "0.7" % "test->default"
)

libraryDependencies += "com.typesafe" %% "scalalogging-slf4j" % "1.0.1"

libraryDependencies += "ch.qos.logback" % "logback-classic" % "1.0.1"

//libraryDependencies += "org.iq80.leveldb" % "leveldb-project" % "0.7-SNAPSHOT"

libraryDependencies += "org.iq80.leveldb" % "leveldb" % "0.6"

//libraryDependencies += "org.fusesource.leveldbjni" % "leveldbjni-all" % "1.7"


libraryDependencies += "org.scala-lang" % "scala-swing" % "2.10.2"

libraryDependencies += "org.scala-lang" %% "scala-pickling" % "0.8.0-SNAPSHOT"

//libraryDependencies += "org.scalatest" % "scalatest_2.10" % "2.0" % "test"

resolvers += Resolver.sonatypeRepo("snapshots")

testOptions += Tests.Argument(TestFrameworks.JUnit, "-v")

javaOptions ++= Seq(
  "-Xms64m",
  "-Xmx256M",
  "-XX:+CMSClassUnloadingEnabled",
  "-XX:MaxPermSize=64M"
)





