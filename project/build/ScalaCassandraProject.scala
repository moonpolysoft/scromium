import sbt._

class ScalaCassandraProject(info : ProjectInfo) extends DefaultProject(info) {
  val codaRepo = "Coda Hale's Repository" at "http://repo.codahale.com/"
  val metrics = "com.yammer" % "metrics_2.8.0.Beta1" % "1.0.2" withSources()
}