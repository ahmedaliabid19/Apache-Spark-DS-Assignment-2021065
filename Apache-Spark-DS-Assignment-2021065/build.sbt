name := "NetflixEDA"
version := "1.0"
scalaVersion := "2.12.17"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.4.0",
  "org.apache.spark" %% "spark-sql" % "3.4.0"
)

fork in run := true
javaOptions ++= Seq(
  "-Xms512M",
  "-Xmx2G"
)
