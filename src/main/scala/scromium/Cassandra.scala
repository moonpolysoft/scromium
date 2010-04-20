package scromium

import connection._
import scromium.reflect.Reflect._
import scromium.util.Log
import java.io._
import scromium.util.JSON
import org.apache.cassandra.config.DatabaseDescriptor
import org.apache.cassandra.db.CompactionManager
import org.apache.cassandra.db.Table
import org.apache.cassandra.io.util.FileUtils
import org.apache.cassandra.db.commitlog.CommitLog
import scala.collection.JavaConversions._
import org.apache.cassandra.service.StorageService
import org.apache.cassandra.net.MessagingService
import org.apache.cassandra.thrift.CassandraServer
import java.lang.management.ManagementFactory
import javax.management._

object Cassandra extends Log {
  def start(file : String) : Cassandra = {
    val config = getConfig(new File(file))
    start(config)
  }
  
  def start(map : Map[String, Any]) : Cassandra = {
    new Cassandra(createConnectionPool(map))
  }
  
  def start() : Cassandra = {    
    val env = System.getenv
    val filePath = env.get("SCROMIUM_CONF")
    val file = new File(filePath, "cassandra.json")
    val config = getConfig(file)
    start(config)
  }
  
  def startTest() : Cassandra = {
    try {
      DatabaseDescriptor.createAllDirectories
      for (table <- DatabaseDescriptor.getTables) {
        println("opening table " + table)
          Table.open(table)
      }
    
/*      CommitLog.recover
      CompactionManager.instance.checkAllColumnFamilies*/
      StorageService.instance.initServer
      val server = new CassandraServer
      val pool =  new ConnectionPool {
        def withConnection[T](block : Client => T) : T = {
          block(new TestClient(server))
        }
      }
      new Cassandra(pool)
    } catch {
      case e : Throwable =>
        println("fuck " + e.getClass)
        e.getCause.printStackTrace
        throw e
    }
  }
  
  private val default = Map("host" -> "localhost", 
    "port" -> 9160, 
    "maxIdle" -> 10, 
    "initCapacity" -> 10,
    "connectionPool" -> "CommonsConnectionPool")
    
  private def createConnectionPool(config : Map[String, Any]) : ConnectionPool = {
    implicit val classLoader = this.getClass.getClassLoader
    
    val claz = config("connectionPool").asInstanceOf[String]
    New(claz)(config, new SocketFactory, new ClusterDiscovery)
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


class Cassandra(connPool : ConnectionPool) {
  def keyspace(name : String) = new Keyspace(name, connPool)
  
  def teardownTest() {
    
    MessagingService.shutdown()
    for (table <- DatabaseDescriptor.getTables) {
      println("closing table " + table)
        Table.close(table)
    }
    
    val server = ManagementFactory.getPlatformMBeanServer
    val query = new QueryExp {
      def apply(name : ObjectName) : Boolean = {
        name.getDomain.startsWith("org.apache.cassandra")
      }
      
      def setMBeanServer(s : MBeanServer) {
        
      }
    }
    for (name <- server.queryNames(null,query).asInstanceOf[java.util.Set[ObjectName]]) {
      server.unregisterMBean(name)
    }
    
    FileUtils.deleteDir(new File(DatabaseDescriptor.getLogFileLocation))
    for (dir <- DatabaseDescriptor.getAllDataFileLocations) {
      FileUtils.deleteDir(new File(dir))
    }
  }
}