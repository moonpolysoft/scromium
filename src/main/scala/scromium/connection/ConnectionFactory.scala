package scromium.connection

import org.apache.commons.pool._
import org.apache.thrift.transport.{TSocket, TTransportException}

class ConnectionFactory(var hosts : Seq[String], val port : Int, socketFactory : SocketFactory) extends PoolableObjectFactory {
  if (hosts.length == 0) {
    throw new IllegalArgumentException("hosts cannot be empty")
  }
  
  //wire up type safe interface with the shitty java one
  override def activateObject(obj : Any) = activate(obj.asInstanceOf[Connection])
  override def destroyObject(obj : Any) = destroy(obj.asInstanceOf[Connection])
  override def makeObject() : Object = make
  override def passivateObject(obj : Any) = passivate(obj.asInstanceOf[Connection])
  override def validateObject(obj : Any) = validate(obj.asInstanceOf[Connection])
  
  def validate(conn : Connection) : Boolean = {
    conn.isOpen
  }
  
  def activate(conn : Connection) {
    conn.ensureOpen
  }
  
  def destroy(conn : Connection) {
    conn.close
  }
  
  def make() : Connection = {
    new Connection(createSocket)
  }
  
  def passivate(conn : Connection) = {
    // noop
  }
  
  def createSocket : TSocket = {
    def createSocket(liveHosts : Seq[String]) : TSocket = liveHosts match {
      case Nil => throw new Exception("No cassandra hosts alive currently.")
      case host :: tail =>
        try {
          val socket = socketFactory.make(host, port)
          if (!socket.isOpen) socket.open
          socket
        } catch {
          case ex : TTransportException => 
            println("ex " + ex.getMessage)
            ex.printStackTrace
            createSocket(tail)
        }
    }
    println("trying to create a socket with hosts list " + hosts)
    val socket = createSocket(hosts)
    val first :: tail = hosts
    hosts = tail ++ List(first)
    socket
  }
}