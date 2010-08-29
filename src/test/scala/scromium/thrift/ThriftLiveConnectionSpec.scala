package scromium.thrift

import scromium.TestHelper
import scromium.client._
import scromium.serializers.Serializers._
import java.net.InetSocketAddress
import java.io.File
import org.specs._
import org.specs.mock.Mockito
import org.mockito.Matchers._
import scala.collection.JavaConversions._
import org.apache.cassandra.service.StorageService
import org.apache.cassandra.net.MessagingService
import org.apache.cassandra.thrift.CassandraServer
import org.apache.cassandra.thrift.Cassandra
import org.apache.cassandra.utils.FBUtilities
import org.apache.cassandra.config.DatabaseDescriptor
import org.apache.cassandra.db.Table
import org.apache.cassandra.io.util.FileUtils
import org.apache.thrift.TProcessorFactory
import org.apache.thrift.protocol.TBinaryProtocol
import org.apache.thrift.protocol.TProtocolFactory
import org.apache.thrift.server.TServer
import org.apache.thrift.transport.TFramedTransport
import org.apache.thrift.transport.TServerSocket
import org.apache.thrift.transport.TTransportException
import org.apache.thrift.transport.TTransportFactory
import org.apache.thrift.server.TThreadPoolServer
import java.lang.management.ManagementFactory
import javax.management._

class ThriftLiveConnectionSpec extends Specification with Mockito with TestHelper {
  var server : TServer = null
  var cassandra : scromium.Cassandra = null
  
  "ThriftClient" should {
    doBefore { cassandra = startCassandraThrift; setupSchema(cassandra) }
    doAfter { teardownCassandraThrift }
    
    "connect to cassandra" in {
      cassandra.keyspace("Keyspace") { ks =>
        ks.columnFamily("ColumnFamily") { cf =>
          cf.putColumn("row", "column", "value")
          
          val result = cf.getColumn("row", "column").get
          result.valueAs[String] must beSome("value")
        }
      }
    }
  }
  
  def startCassandraThrift : scromium.Cassandra = {
    try {
      DatabaseDescriptor.createAllDirectories
      DatabaseDescriptor.loadSchemas
      for (table <- DatabaseDescriptor.getTables) {
          Table.open(table)
      }
      StorageService.instance.initServer
      val listenPort = DatabaseDescriptor.getRpcPort
      var listenAddr = DatabaseDescriptor.getRpcAddress
      if (listenAddr == null) listenAddr = FBUtilities.getLocalAddress
      val cassandraServer = new CassandraServer
      val processor = new Cassandra.Processor(cassandraServer)
      val serverSocket = new TServerSocket(new InetSocketAddress(listenAddr, listenPort))
      val protocolFactory = new TBinaryProtocol.Factory(true, true)
      server = new TThreadPoolServer(processor, serverSocket)
      println("STARTING SERVER")
      
      new Thread {
        override def run = {
          server.serve
        }
      }.start
      Thread.sleep(100)
      
      scromium.Cassandra.start(Map(
        "clientProvider" -> "scromium.thrift.ThriftClientProvider",
        "seedHost" -> "localhost",
        "seedPort" -> 9160,
        "maxIdle" -> 10,
        "initCapacity" -> 10
      ))
    } catch {
      case e : Throwable =>
        println("fuck " + e.getClass)
        if (e.getCause != null) e.getCause.printStackTrace
        e.printStackTrace
        throw e
    }
  }
  
  def teardownCassandraThrift {
    server.stop
    MessagingService.shutdown()
    for (table <- DatabaseDescriptor.getTables) {
        Table.clear(table)
    }
    
    val mbeanServer = ManagementFactory.getPlatformMBeanServer
    val query = new QueryExp {
      def apply(name : ObjectName) : Boolean = {
        name.getDomain.startsWith("org.apache.cassandra")
      }
      
      def setMBeanServer(s : MBeanServer) {
        
      }
    }
    for (name <- mbeanServer.queryNames(null,query).asInstanceOf[java.util.Set[ObjectName]]) {
      mbeanServer.unregisterMBean(name)
    }
    
    FileUtils.deleteRecursive(new File(DatabaseDescriptor.getCommitLogLocation))
    for (dir <- DatabaseDescriptor.getAllDataFileLocations) {
      FileUtils.deleteRecursive(new File(dir))
    }
  }
}