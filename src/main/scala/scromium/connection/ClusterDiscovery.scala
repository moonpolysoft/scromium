package scromium.connection

import org.apache.thrift.protocol.TBinaryProtocol
import org.apache.thrift.transport.{TSocket, TTransportException}
import org.apache.cassandra.thrift
import scromium.util.JSON

class ClusterDiscovery {
  @throws(classOf[TTransportException])
  def hosts(seedHost : String, seedPort : Int) : Seq[String] = {
    val socket = new TSocket(seedHost, seedPort)
    socket.open
    val client = new thrift.Cassandra.Client(new TBinaryProtocol(socket))
    val tokenMap = JSON.parseObject(client.get_string_property("token map"))
    socket.close
    val seq = for ((key, value) <- tokenMap if value.isInstanceOf[String]) yield(value.asInstanceOf[String])
    seq.toSeq.distinct
  }
}
