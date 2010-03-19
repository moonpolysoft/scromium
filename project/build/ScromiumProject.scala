import sbt._

class ScromiumProject(info : ProjectInfo) extends DefaultProject(info) with BasicScalaIntegrationTesting {
  val codaRepo = "Coda Hale's Repository" at "http://repo.codahale.com/"
  val jetlangRepo = "Jet Lang Repository" at "http://jetlang.googlecode.com/svn/repo/"
  
  val metrics = "com.yammer" % "metrics_2.8.0.Beta1" % "1.0.2" withSources()
  val guild = "com.codahale" % "guild_2.8.0.Beta1" % "1.0-SNAPSHOT" withSources()
  val jetlang = "org.jetlang" % "jetlang" % "0.2.0" withSources()
  val pool = "commons-pool" % "commons-pool" % "1.5.4" withSources() intransitive()
  val slf4japi = "org.slf4j" % "slf4j-api" % "1.5.11" withSources() intransitive()
  val slf4j = "org.slf4j" % "slf4j-jdk14" % "1.5.11" withSources() intransitive()
  
  val mockito = "org.mockito" % "mockito-all" % "1.8.1" % "test" withSources()
}
