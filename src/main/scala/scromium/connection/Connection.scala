package scromium.connection

import org.apache.thrift.transport.TSocket
import org.apache.thrift.protocol.TBinaryProtocol
import org.apache.cassandra.thrift

class Connection(val socket : TSocket) {
  val protocol = new TBinaryProtocol(socket)
  val client = new thrift.Cassandra.Client(protocol)
  
  def isOpen() : Boolean = {socket.isOpen()}
  
  def ensureOpen {
    if (!socket.isOpen) {
      socket.open
    }
  }
  
  def close {
    socket.close
  }
}
