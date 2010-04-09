package scromium.connection

import org.apache.thrift.transport.TSocket
import org.apache.thrift.protocol.TBinaryProtocol
import org.apache.cassandra.thrift._
import java.util.List
import java.util.Map

class Connection(val socket : TSocket) extends Client {
  private val protocol = new TBinaryProtocol(socket)
  protected[connection] val client = new Cassandra.Client(protocol)
  
  def isOpen() : Boolean = {socket.isOpen()}
  
  def ensureOpen {
    if (!socket.isOpen) {
      socket.open
    }
  }
  
  def close {
    socket.close
  }
  
  override def get(ks : String, k : String, cp : ColumnPath, c : ConsistencyLevel) : ColumnOrSuperColumn = {
    client.get(ks, k, cp, c)
  }
  
  override def multiget_slice(ks : String, keys : List[String], cp : ColumnParent, p : SlicePredicate, c : ConsistencyLevel) : Map[String,List[ColumnOrSuperColumn]] = {
    client.multiget_slice(ks, keys, cp, p, c)
  }
  
  override def get_range_slices(ks : String, cp : ColumnParent, p : SlicePredicate, kr : KeyRange, c : ConsistencyLevel) : List[KeySlice] = {
    client.get_range_slices(ks, cp, p, kr, c)
  }
  
  override def insert(ks : String, key : String, cp : ColumnPath, value : Array[Byte], timestamp : Long, c : ConsistencyLevel) {
    client.insert(ks, key, cp, value, timestamp, c)
  }
  
  override def batch_mutate(ks : String, map : Map[String,Map[String,List[Mutation]]], c : ConsistencyLevel) {
    client.batch_mutate(ks, map, c)
  }
}
