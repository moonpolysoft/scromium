package scromium.thrift

import org.apache.commons.pool._
import org.apache.cassandra.thrift.Cassandra
import org.apache.thrift.transport.{TTransport, TSocket, TTransportException}
import org.apache.thrift.protocol.TBinaryProtocol
import scromium.util.Log

class ThriftClientFactory(var hosts : Seq[String], 
    val port : Int, 
    socketFactory : ThriftSocketFactory) extends PoolableObjectFactory with Log {
  if (hosts.length == 0) {
    throw new IllegalArgumentException("hosts cannot be empty")
  }
  
  //wire up type safe interface with the shitty java one
  override def activateObject(obj : Any) = activate(obj.asInstanceOf[ThriftConnection])
  override def destroyObject(obj : Any) = destroy(obj.asInstanceOf[ThriftConnection])
  override def makeObject() : Object = make
  override def passivateObject(obj : Any) = passivate(obj.asInstanceOf[ThriftConnection])
  override def validateObject(obj : Any) = validate(obj.asInstanceOf[ThriftConnection])
  
  def validate(conn : ThriftConnection) : Boolean = {
    conn.isOpen
  }
  
  def activate(conn : ThriftConnection) {
    conn.ensureOpen
  }
  
  def destroy(conn : ThriftConnection) {
    conn.close
  }
  
  def make() : ThriftConnection = {
    val socket = createSocket
    val client = createClient(socket)
    new ThriftConnection(createSocket, client)
  }
  
  def passivate(conn : ThriftConnection) = {
    // noop
  }
  
  def createClient(socket : TTransport) : Cassandra.Client = {
    val protocol = new TBinaryProtocol(socket)
    new Cassandra.Client(protocol)
  }
  
  def createSocket : TTransport = {
    def createSocket(liveHosts : Seq[String]) : TTransport = liveHosts match {
      case Nil => throw new Exception("No cassandra hosts alive currently.")
      case host :: tail =>
        try {
          val socket = socketFactory.make(host, port)
          if (!socket.isOpen) socket.open
          socket
        } catch {
          case ex : TTransportException =>
            error("Error creating socket", ex)
            createSocket(tail)
        }
    }
    synchronized {
      val socket = createSocket(hosts)
      val first :: tail = hosts
      hosts = tail ++ List(first)
      socket
    }
  }
}