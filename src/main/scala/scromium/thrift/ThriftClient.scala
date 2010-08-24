package scromium.thrift

import scromium._
import scromium.meta._
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
  
  def put(keyspace : String, writes : List[Write[Column]], c : WriteConsistency) {
    cass.set_keyspace(keyspace)
    val rowMap = createRowMap
    writes.foreach { write =>
      val mutes = rowMap.get(write.key).get(write.cf)
      mutes ++= write.columns.map(columnMutation(_))
    }
    cass.batch_mutate(rowMap, c.thrift)
  }
  
  def superPut(keyspace : String, writes : List[Write[SuperColumn]], c : WriteConsistency) {
    cass.set_keyspace(keyspace)
    val rowMap = createRowMap
    writes.foreach { write =>
      val mutes = rowMap.get(write.key).get(write.cf)
      mutes ++= write.columns.map(superColumnMutation(_))
    }
    cass.batch_mutate(rowMap, c.thrift)
  }
  
  def delete(keyspace : String, delete : Delete, c : WriteConsistency) {
    cass.set_keyspace(keyspace)
    val rowMap = createRowMap
    val mutation = deleteMutation(delete)
    delete.keys.foreach { key =>
      rowMap.get(key).get(delete.cf) += mutation
    }
    cass.batch_mutate(rowMap, c.thrift)
  }
  
  def get(keyspace : String, read : Read, c : ReadConsistency) : RowIterator[Column] = {
    cass.set_keyspace(keyspace)
    val parent = readToColumnParent(read)
    val predicate = readToPredicate(read)
    val results = cass.multiget_slice(read.keys, parent, predicate, c.thrift)
    new MGColumnRowIterator(results)
  }
  
  def superGet(keyspace : String, read : Read, c : ReadConsistency) : RowIterator[SuperColumn] = {
    cass.set_keyspace(keyspace)
    val parent = readToColumnParent(read)
    val predicate = readToPredicate(read)
    val results = cass.multiget_slice(read.keys, parent, predicate, c.thrift)
    new MGSuperColumnRowIterator(results)
  }
  
  def createKeyspace(keyspace : KeyspaceDef) {
    cass.system_add_keyspace(ksDef(keyspace))
  }
  
  def createColumnFamily(cf : ColumnFamilyDef) {
    cass.system_add_column_family(cfDef(cf))
  }
  
  def dropKeyspace(name : String) {
    cass.system_drop_keyspace(name)
  }
  
  def renameKeyspace(from : String, to : String) {
    cass.system_rename_keyspace(from, to)
  }
  
  def dropColumnFamily(name : String) {
    cass.system_drop_column_family(name)
  }
  
  def renameColumnFamily(from : String, to : String) {
    cass.system_rename_column_family(from, to)
  }
  
/*  def scan(scanner : Scanner[Column], c : ReadConsistency) : RowIterator[Column]
  def superScan(scanner : Scanner[SuperColumn], c : ReadConsistency) : RowIterator[SuperColumn]*/
  
  private def createRowMap = new DefaultHashMap[Array[Byte], MuteMap]({ key =>
    new MapWrapper(new DefaultHashMap[String, JList[thrift.Mutation]]({ cf =>
      new JListWrapper(new ListBuffer[thrift.Mutation])
    }))
  })
}
