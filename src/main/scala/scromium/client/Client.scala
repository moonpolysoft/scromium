package scromium.client

import scromium._
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
  def put(rows : List[Write[Column]], c : WriteConsistency)
  def superPut(rows : List[Write[SuperColumn]], c : WriteConsistency)
  def delete(delete : Delete, c : WriteConsistency)
  def get(read : Read, c : ReadConsistency) : RowIterator[Column]
  def superGet(read : Read, c : ReadConsistency) : RowIterator[SuperColumn]
/*  def scan(scanner : Scanner[Column], c : ReadConsistency) : RowIterator[Column]
  def superScan(scanner : Scanner[SuperColumn], c : ReadConsistency) : RowIterator[SuperColumn]*/
}

class JMXClient(cl : Client) extends Client {
  def put(rows : List[Write[Column]], c : WriteConsistency) {
    putLoad.mark(rows.size)
    putTimer.time { cl.put(rows, c) }
  }
  
  def superPut(rows : List[Write[SuperColumn]], c : WriteConsistency) {
    putLoad.mark(rows.size)
    putTimer.time { cl.superPut(rows, c) }
  }
  
  def delete(delete : Delete, c : WriteConsistency) {
    deleteLoad.mark(delete.keys.size)
    deleteTimer.time { cl.delete(delete, c) }
  }
  
  def get(read : Read, c : ReadConsistency) : RowIterator[Column] = {
    getLoad.mark(read.keys.size)
    getTimer.time { cl.get(read, c) }
  }
  
  def superGet(read : Read, c : ReadConsistency) : RowIterator[SuperColumn] = {
    getLoad.mark(read.keys.size)
    getTimer.time { cl.superGet(read, c) }
  }
  
/*  def scan(scanner : Scanner[Column], c : ReadConsistency) : RowIterator[Column] = {
    scanLoad.mark(1)
    scanTimer.time { cl.scan(scanner, c) }
  }
  
  def superScan(scanner : Scanner[SuperColumn], c : ReadConsistency) : RowIterator[SuperColumn] = {
    scanLoad.mark(1)
    scanTimer.time { cl.superScan(scanner, c) }
  }*/
}