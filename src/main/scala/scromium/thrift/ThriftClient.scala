package scromium.thrift

import scromium._
import scromium.client._
import scromium.util.DefaultHashMap
import org.apache.cassandra.thrift
import org.apache.thrift.transport.{TTransport, TTransportException}
import java.util.{Map => JMap, List => JList}
import scala.collection.mutable.ListBuffer
import scala.collection.JavaConversions._
import Thrift._

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
  type MuteMap = JMap[String, JList[thrift.Mutation]]
  
    def put(writes : List[Write[Column]], c : WriteConsistency) {
      val rowMap = createRowMap
      writes.foreach { write =>
        val mutes = rowMap.get(write.key).get(write.cf)
        mutes ++= write.columns.map(columnMutation(_))
      }
      cass.batch_mutate(rowMap, c.thrift)
    }
    
    def superPut(writes : List[Write[SuperColumn]], c : WriteConsistency) {
      val rowMap = createRowMap
      writes.foreach { write =>
        val mutes = rowMap.get(write.key).get(write.cf)
        mutes ++= write.columns.map(superColumnMutation(_))
      }
      cass.batch_mutate(rowMap, c.thrift)
    }
    
    def delete(delete : Delete, c : WriteConsistency) {
      val rowMap = createRowMap
      val mutation = deleteMutation(delete)
      delete.keys.foreach { key =>
        rowMap.get(key).get(delete.cf) += mutation
      }
      cass.batch_mutate(rowMap, c.thrift)
    }
    
    def get(read : Read, c : ReadConsistency) : RowIterator[Column] = {
      val parent = readToColumnParent(read)
      val predicate = readToPredicate(read)
      val results = cass.multiget_slice(read.keys, parent, predicate, c.thrift)
      new MGColumnRowIterator(results)
    }
    
    def superGet(read : Read, c : ReadConsistency) : RowIterator[SuperColumn] = {
      val parent = readToColumnParent(read)
      val predicate = readToPredicate(read)
      val results = cass.multiget_slice(read.keys, parent, predicate, c.thrift)
      new MGSuperColumnRowIterator(results)
    }
    
  /*  def scan(scanner : Scanner[Column], c : ReadConsistency) : RowIterator[Column]
    def superScan(scanner : Scanner[SuperColumn], c : ReadConsistency) : RowIterator[SuperColumn]*/
    
    private def createRowMap = new DefaultHashMap[Array[Byte], MuteMap]({ key =>
      new MapWrapper(new DefaultHashMap[String, JList[thrift.Mutation]]({ cf =>
        new JListWrapper(new ListBuffer[thrift.Mutation])
      }))
    })
}
