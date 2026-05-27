ThisBuild / scalaVersion := "3.7.3"

lazy val root = (project in file("."))
  .settings(
    name := "stox-kafka",
    libraryDependencies ++= Seq(
      "org.apache.kafka" % "kafka-clients" % "4.3.0",
      "org.slf4j" % "slf4j-simple" % "2.0.17",
      "com.lihaoyi" %% "ujson" % "4.3.2",
      "org.apache.parquet" % "parquet-avro" % "1.15.2",
      "org.apache.hadoop" % "hadoop-common" % "3.4.1",
      "org.xerial" % "sqlite-jdbc" % "3.50.3.0",
      "org.apache.pekko" %% "pekko-actor-typed" % "1.3.0",
      "org.apache.pekko" %% "pekko-stream" % "1.3.0",
      "org.apache.pekko" %% "pekko-http" % "1.3.0",
      "org.apache.hadoop" % "hadoop-mapreduce-client-core" % "3.4.1"
    )
  )
