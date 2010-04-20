import sbt._
import sbt.StringUtilities._

class ScromiumProject(info : ProjectInfo) extends DefaultProject(info) with BasicScalaIntegrationTesting with AssemblyAction {
  override def compileOptions = Deprecation :: Unchecked :: super.compileOptions.toList
  
  override def managedStyle = ManagedStyle.Maven
  val publishTo = Resolver.file("gh-pages", new java.io.File("/Users/cliff/projects/scromium-pages/repository"))
  override def defaultMainArtifact = Artifact("scromium-core" + appendable(crossScalaVersionString), "core", "jar")
  val all = Artifact("scromium-all" + appendable(crossScalaVersionString), "all", "jar")
  override def packageToPublishActions = super.packageToPublishActions ++ Seq(assembly)
  
  val codaRepo = "Coda Hale's Repository" at "http://repo.codahale.com/"
  val jetlangRepo = "Jet Lang Repository" at "http://jetlang.googlecode.com/svn/repo/"
  
  val metrics = "com.yammer" % "metrics_2.8.0.Beta1" % "1.0.2" withSources()
  val guild = "com.codahale" % "guild_2.8.0.Beta1" % "1.1-SNAPSHOT"
  val jetlang = "org.jetlang" % "jetlang" % "0.2.0" withSources()
  val pool = "commons-pool" % "commons-pool" % "1.5.4" withSources() intransitive()
  val codec = "commons-codec" % "commons-codec" % "1.4"
  val slf4japi = "org.slf4j" % "slf4j-api" % "1.5.11" withSources() intransitive()
  val slf4j = "org.slf4j" % "slf4j-log4j12" % "1.5.11" withSources()
  //cassandra deps
  val collections = "com.google.collections" % "google-collections" % "1.0"
  val cc = "commons-collections" % "commons-collections" % "3.2.1"
  val lang = "commons-lang" % "commons-lang" % "2.4"
  
  val mockito = "org.mockito" % "mockito-all" % "1.8.1" % "test" withSources()
}

/**
 * Builds a single-file, deployable artifact with optimized code.
 *
 * @author coda
 * @see http://technically.us/git?p=sling.git;a=blob;f=project/build/AssemblyProject.scala;hb=HEAD
 */
trait AssemblyAction extends sbt.BasicScalaProject {
  override def classpathFilter = super.classpathFilter -- "*-sources.jar" -- "*-javadoc.jar"
  override def compileOptions = super.compileOptions ++ List(Deprecation, Unchecked) ++ assemblyOptions
  private var useOptimization = false
  def assemblyOptions = if (useOptimization) {
    List(Optimize)
  } else Nil

  def assemblyExclude(base: sbt.PathFinder) = base / "META-INF" ** "*"
  def assemblyOutputPath = outputPath / assemblyJarName
  def assemblyJarName = name + "-all" + appendable(crossScalaVersionString) + "-" + this.version + ".jar"
  def assemblyTemporaryPath = outputPath / "assembly-libs"
  def assemblyClasspath = runClasspath
  def assemblyExtraJars = mainDependencies.scalaJars

  def assemblyPaths(tempDir: sbt.Path, classpath: sbt.PathFinder, extraJars: sbt.PathFinder, exclude: sbt.PathFinder => sbt.PathFinder) = {
    val (libs, _) = classpath.get.toList.partition { dir =>
      dir.toString.startsWith("./lib/") && sbt.ClasspathUtilities.isArchive(dir) && !dir.toString.contains("specs")
    }
    val (_, directories) = classpath.get.toList.partition(sbt.ClasspathUtilities.isArchive)
    for(jar <- libs ++ extraJars.get) {
      val jarName = jar.asFile.getName
      log.info("Including %s".format(jarName))
      sbt.FileUtilities.unzip(jar, tempDir, log).left.foreach(error)
    }
    val base = (sbt.Path.lazyPathFinder(tempDir :: directories) ##)
    val paths = (descendents(base, "*") --- exclude(base)).get
    paths
  }

  def assemblyTask(tempDir: sbt.Path, classpath: sbt.PathFinder, extraJars: sbt.PathFinder, exclude: sbt.PathFinder => sbt.PathFinder) = {
    packageTask(sbt.Path.lazyPathFinder(assemblyPaths(tempDir, classpath, extraJars, exclude)), assemblyOutputPath, packageOptions)
  }

  /**
   * Compiles classes and tests normally, runs the tests, then cleans and
   * recompiles with optimizations turned on and produces a single-file
   * build JAR.
   */
  lazy val assembly = task {
    try {
      assemblyTask(assemblyTemporaryPath, assemblyClasspath, assemblyExtraJars, assemblyExclude).run
    } finally {
      useOptimization = false
    }
    None
  } dependsOn(test) describedAs("Builds an optimized, single-file deployable JAR.")
}