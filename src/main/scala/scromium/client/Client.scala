package scromium.client

import scromium._
import scromium.meta._
import com.yammer.jmx._
import com.yammer.metrics._
import java.util.concurrent.TimeUnit

object ClientStats extends JmxManaged {
  val getTimer = new Timer
  val putTimer = new Timer
  val deleteTimer = new Timer
  val scanTimer = new Timer
  
  val getLoad = new LoadMeter
  val putLoad = new LoadMeter
  val deleteLoad = new LoadMeter
  val scanLoad = new LoadMeter
  
  
  enableJMX("Cassandra") { jmx =>
    jmx.addMeter("gets", getLoad, TimeUnit.SECONDS)
    jmx.addMeter("puts", putLoad, TimeUnit.SECONDS)
    jmx.addMeter("deletes", deleteLoad, TimeUnit.SECONDS)
    jmx.addMeter("scans", scanLoad, TimeUnit.SECONDS)
    
    jmx.addTimer("get_latency", getTimer, TimeUnit.SECONDS)
    jmx.addTimer("put_latency", putTimer, TimeUnit.SECONDS)
    jmx.addTimer("delete_latency", deleteTimer, TimeUnit.SECONDS)
    jmx.addTimer("scan_latency", scanTimer, TimeUnit.SECONDS)
  }
}

import ClientStats._

trait Client {
  def put(keyspace : String, rows : List[Write[Column]], c : WriteConsistency)
  def superPut(keyspace : String, rows : List[Write[SuperColumn]], c : WriteConsistency)
  def delete(keyspace : String, delete : Delete, c : WriteConsistency)
  def get(keyspace : String, read : Read, c : ReadConsistency) : RowIterator[Column]
  def superGet(keyspace : String, read : Read, c : ReadConsistency) : RowIterator[SuperColumn]
  def createKeyspace(keyspace : KeyspaceDef)
  def createColumnFamily(cf : ColumnFamilyDef)
  def dropKeyspace(name : String)
  def renameKeyspace(from : String, to : String)
  def dropColumnFamily(name : String)
  def renameColumnFamily(from : String, to : String)
  def listKeyspaces : Set[String]
/*  def scan(scanner : Scanner[Column], c : ReadConsistency) : RowIterator[Column]
  def superScan(scanner : Scanner[SuperColumn], c : ReadConsistency) : RowIterator[SuperColumn]*/
}

class JMXClient(cl : Client) extends Client {
  def put(keyspace : String, rows : List[Write[Column]], c : WriteConsistency) {
    putLoad.mark(rows.size)
    putTimer.time { cl.put(keyspace, rows, c) }
  }
  
  def superPut(keyspace : String, rows : List[Write[SuperColumn]], c : WriteConsistency) {
    putLoad.mark(rows.size)
    putTimer.time { cl.superPut(keyspace, rows, c) }
  }
  
  def delete(keyspace : String, delete : Delete, c : WriteConsistency) {
    deleteLoad.mark(delete.keys.size)
    deleteTimer.time { cl.delete(keyspace, delete, c) }
  }
  
  def get(keyspace : String, read : Read, c : ReadConsistency) : RowIterator[Column] = {
    getLoad.mark(read.keys.size)
    getTimer.time { cl.get(keyspace, read, c) }
  }
  
  def superGet(keyspace : String, read : Read, c : ReadConsistency) : RowIterator[SuperColumn] = {
    getLoad.mark(read.keys.size)
    getTimer.time { cl.superGet(keyspace, read, c) }
  }
  
  def createKeyspace(keyspace : KeyspaceDef) = cl.createKeyspace(keyspace)
  def createColumnFamily(cf : ColumnFamilyDef) = cl.createColumnFamily(cf)
  def dropKeyspace(name : String) = cl.dropKeyspace(name)
  def renameKeyspace(from : String, to : String) = cl.renameKeyspace(from, to)
  def dropColumnFamily(name : String) = cl.dropColumnFamily(name)
  def renameColumnFamily(from : String, to : String) = cl.renameColumnFamily(from, to)
  def listKeyspaces : Set[String] = cl.listKeyspaces
  
/*  def scan(scanner : Scanner[Column], c : ReadConsistency) : RowIterator[Column] = {
    scanLoad.mark(1)
    scanTimer.time { cl.scan(scanner, c) }
  }
  
  def superScan(scanner : Scanner[SuperColumn], c : ReadConsistency) : RowIterator[SuperColumn] = {
    scanLoad.mark(1)
    scanTimer.time { cl.superScan(scanner, c) }
  }*/
}