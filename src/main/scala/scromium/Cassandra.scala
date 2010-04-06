package scromium

import connection._
import scromium.reflect.Reflect._
import scromium.util.Log
import java.io._
import scromium.util.JSON

object Cassandra extends Log {
  def start(file : String) {
    val config = getConfig(new File(file))
    start(config)
  }
  
  def start(map : Map[String, Any]) {
    Keyspace.pool = createConnectionPool(map)
  }
  
  def start() {    
    val env = System.getenv
    val filePath = env.get("SCROMIUM_CONF")
    val file = new File(filePath, "cassandra.json")
    val config = getConfig(file)
    start(config)
  }
  
  private val default = Map("host" -> "localhost", 
    "port" -> 9160, 
    "maxIdle" -> 10, 
    "initCapacity" -> 10,
    "connectionPool" -> "CommonsConnectionPool")
    
  private def createConnectionPool(config : Map[String, Any]) : ConnectionPool = {
    implicit val classLoader = this.getClass.getClassLoader
    
    val claz = config("connectionPool").asInstanceOf[String]
    New(claz)(config)
  }
  
  private def getConfig(file : File) : Map[String, Any] = {
    try {
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
    def readAll(reader : BufferedReader, acc : String) : String = {
      reader.readLine match {
        case v : String => readAll(reader, acc + v)
        case _ => acc
      }
    }
    
    val reader = new BufferedReader(new FileReader(file))
    val builder = new StringBuilder
    val contents = readAll(reader, "")
    reader.close
    contents
  }
}
