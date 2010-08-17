package scromium.thrift

import scromium._
import scromium.client._
import org.apache.cassandra.thrift
import org.apache.thrift.transport.{TSocket, TTransportException}
import java.util.{HashMap, List, ArrayList}
import scala.collection.JavaConversions._

class ThriftConnection(socket : TSocket, client : thrift.Cassandra.Client) extends ThriftClient(client) {
  
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
  
  def put(rows : List[Row[Column]], consistency : WriteConsistency) {
/*    val rowMutations = new MuteMap
    for (r <- rows) {
      val mutations = r.columns.map { c =>
        val m = new thrift.Mutation
        val corsc = new thrift.ColumnOrSuperColumn
        val tc = new thrift.Column
        tc.name = c.name
        tc.value = c.value
        tc.clock = new thrift.Clock(c.timestamp)
        for (ttl <- c.ttl) tc.ttl = ttl
        corsc.column = tc
        m.column_or_supercolumn = corsc
        m
      }
      rowMutations.put(r.key, mutations)
    }
    cass.batch_mutate(rowMutations, consistency)*/
  }
  
  def superPut(rows : List[Row[SuperColumn]], c : WriteConsistency) {
    
  }
  
  def delete(rows : List[RowDeletion[Column]], c : WriteConsistency) {
    
  }
  
  def superDelete(rows : List[RowDeletion[SuperColumn]], c : WriteConsistency) {
    
  }
  
  def get(reads : List[Read[Column]], c : ReadConsistency) : RowIterator[Column] = {
    null
  }
  
  def superGet(reads : List[Read[SuperColumn]], c : ReadConsistency) : RowIterator[SuperColumn] = {
    null
  }
  
  def scan(scanner : Scanner[Column], c : ReadConsistency) : RowIterator[Column] = {
    null
  }
  
  def superScan(scanner : Scanner[SuperColumn], c : ReadConsistency) : RowIterator[SuperColumn] = {
    null
  }
}
