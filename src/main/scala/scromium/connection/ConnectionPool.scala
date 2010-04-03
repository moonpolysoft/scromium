package scromium.connection

import org.apache.commons.pool._
import org.apache.commons.pool.impl._
import java.io._
import scromium.util.JSON
import scromium.util.Log

object ConnectionPool extends Log {
  private val default = Map("host" -> "localhost", 
    "port" -> 9160, 
    "maxIdle" -> 10, 
    "initCapacity" -> 10)
  
  def createConnectionPool() : ConnectionPool = {
    implicit def anyToInt(a : Any) = a match {
      case s : String => s.toInt
      case i : Int => i
    }
    val config = getConfig
    new ConcreteConnectionPool(config("host").asInstanceOf[String],
      config("port"),
      config("maxIdle"),
      config("initCapacity"))
  }
  
  
  private def getConfig() : Map[String, Any] = {
    try {
      val env = System.getenv
      val filePath = env.get("SCROMIUM_CONF")
      val file = new File(filePath, "cassandra.json")
      if (file.isFile) {
        val json = readFile(file)
        info("config " + json)
        JSON.parseObject(json)
      } else {
        info("config file " + file + " does not exist")
        default
      }
    } catch {
      case _ => default
    }
  }
  
  private def readFile(file : File) : String = {
    val reader = new BufferedReader(new FileReader(file))
    val builder = new StringBuilder
    var line = null
    while ((line = reader.readLine) != null) {
      builder ++= line
    }
    reader.close
    builder.toString
  }
}

trait ConnectionPool {
  def withConnection[T](block : Connection => T) : T
  def borrow : Connection
  def returnConnection(conn : Connection)
}

class ConcreteConnectionPool(val seedHost : String, 
    val seedPort : Int, 
    val maxIdle : Int, 
    val initCapacity : Int, 
    socketFactory : SocketFactory = new SocketFactory, 
    clusterDiscovery : ClusterDiscovery = new ClusterDiscovery) extends ConnectionPool with Log {
  
  val hosts = clusterDiscovery.hosts(seedHost,seedPort)
  val objectPool = new StackObjectPool(new ConnectionFactory(hosts, seedPort, socketFactory), maxIdle, initCapacity)
  
  def withConnection[T](block : Connection => T) : T = {
    var connection : Connection = null
    try {
      connection = borrow
      block(connection)
    } catch {
      case ex : Throwable =>
        error("Error while trying to use connection", ex)
        throw ex
    } finally {
      if (connection != null) returnConnection(connection)
    }
  }
  
  def borrow : Connection = {
    objectPool.borrowObject.asInstanceOf[Connection]
  }
  
  def returnConnection(conn : Connection) {
    objectPool.returnObject(conn)
  }
}
