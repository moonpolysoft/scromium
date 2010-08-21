package scromium.client

import scromium._
import com.yammer.jmx._
import com.yammer.metrics._
import java.util.concurrent.TimeUnit
import java.util.List
import java.util.Map

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
  def delete(rows : List[Delete], c : WriteConsistency)
  def get(reads : List[Read], c : ReadConsistency) : RowIterator[Column]
  def superGet(reads : List[Read], c : ReadConsistency) : RowIterator[SuperColumn]
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
  
  def delete(rows : List[Delete], c : WriteConsistency) {
    deleteLoad.mark(rows.size)
    deleteTimer.time { cl.delete(rows, c) }
  }
  
  def get(reads : List[Read], c : ReadConsistency) : RowIterator[Column] = {
    getLoad.mark(reads.size)
    getTimer.time { cl.get(reads, c) }
  }
  
  def superGet(reads : List[Read], c : ReadConsistency) : RowIterator[SuperColumn] = {
    getLoad.mark(reads.size)
    getTimer.time { cl.superGet(reads, c) }
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