lazy val root = (project in file(".")).
  settings(
    name := "spark-job",
    version := "1.0",
    scalaVersion := "2.11.8",
    libraryDependencies ++= Seq("org.apache.spark" %% "spark-core" % "2.1.0" % "provided",
                                "org.apache.spark" %% "spark-streaming" % "2.1.0" % "provided",
                                "org.apache.spark" %% "spark-streaming-kafka-0-8" % "2.1.0",
                                "org.apache.spark" %% "spark-mllib" % "2.1.0",
                                "org.elasticsearch" %% "elasticsearch-spark-20" % "5.4.0",
                                "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.8.8",
                                "com.fasterxml.jackson.core" % "jackson-databind" % "2.8.8",
                                "com.fasterxml.jackson.datatype" % "jackson-datatype-joda" % "2.8.0",
                                "joda-time" % "joda-time" % "2.9.4"
                                       ),
    resolvers ++= Seq(Resolver.mavenLocal,
                      Resolver.sbtPluginRepo("releases"),
                      Resolver.sonatypeRepo("public"),
                      Resolver.sonatypeRepo("releases")
                             ),
    mainClass in assembly := Some("com.proofpoint.ReadKinesisStream"),
    assemblyJarName in assembly := "spark-job.jar")

assemblyMergeStrategy in assembly := {
  case PathList("org", "aopalliance", xs@_*) => MergeStrategy.last
  case PathList("javax", "inject", xs@_*) => MergeStrategy.last
  case PathList("javax", "servlet", xs@_*) => MergeStrategy.last
  case PathList("javax", "activation", xs@_*) => MergeStrategy.last
  case PathList("org", "apache", xs@_*) => MergeStrategy.last
  case PathList("com", "google", xs@_*) => MergeStrategy.last
  case PathList("com", "esotericsoftware", xs@_*) => MergeStrategy.last
  case PathList("com", "codahale", xs@_*) => MergeStrategy.last
  case PathList("com", "yammer", xs@_*) => MergeStrategy.last
  case "about.html" => MergeStrategy.rename
  case "META-INF/ECLIPSEF.RSA" => MergeStrategy.last
  case "META-INF/mailcap" => MergeStrategy.last
  case "META-INF/mimetypes.default" => MergeStrategy.last
  case "plugin.properties" => MergeStrategy.last
  case "log4j.properties" => MergeStrategy.last
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}
