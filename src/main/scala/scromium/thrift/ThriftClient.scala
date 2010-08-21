package scromium.thrift

import scromium._
import scromium.client._
import org.apache.cassandra.thrift
import org.apache.thrift.transport.{TTransport, TTransportException}
import java.util.{HashMap, List, ArrayList}
import scala.collection.JavaConversions._

class ThriftConnection(socket : TTransport, client : thrift.Cassandra.Client) extends ThriftClient(client) {
  
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

class ThriftClient(cass : thrift.Cassandra.Iface) extends Client {
  type MuteMap = HashMap[Array[Byte], List[thrift.Mutation]]
  
    def put(rows : List[Write[Column]], c : WriteConsistency) {
      
    }
    
    def superPut(rows : List[Write[SuperColumn]], c : WriteConsistency) {
      
    }
    
    def delete(rows : List[Delete], c : WriteConsistency) {
      
    }
    
    def get(reads : List[Read], c : ReadConsistency) : RowIterator[Column] = {
      null
    }
    
    def superGet(reads : List[Read], c : ReadConsistency) : RowIterator[SuperColumn] = {
      null
    }
    
  /*  def scan(scanner : Scanner[Column], c : ReadConsistency) : RowIterator[Column]
    def superScan(scanner : Scanner[SuperColumn], c : ReadConsistency) : RowIterator[SuperColumn]*/
}
