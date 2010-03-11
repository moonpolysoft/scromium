package scromium.connection

import org.apache.thrift.transport.{TSocket, TTransportException}

class SocketFactory {
  @throws(classOf[TTransportException])
  def make(host : String, port : Int) : TSocket = {
    new TSocket(host, port)
  }
}
