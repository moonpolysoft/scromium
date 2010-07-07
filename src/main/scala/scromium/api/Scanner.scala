package scromium.api

import org.apache.cassandra.thrift
import scala.collection._
import scromium._
import scromium.util._
import java.util.NoSuchElementException
import scala.collection.JavaConversions._

abstract class Scanner[T](ks : Keyspace,
                 cp : thrift.ColumnParent,
                 predicate : thrift.SlicePredicate,
                 range : thrift.KeyRange,
                 consistency : ReadConsistency) extends Iterable[Row[T]] {
  
  def converter : thrift.ColumnOrSuperColumn => T
  
  def iterator = new ScannerIterator[T](converter)
  
  class ScannerIterator[T](converter : thrift.ColumnOrSuperColumn => T) extends Iterator[Row[T]] {
                             
    private var stream = ScanStream[T](ks, cp, predicate, range, consistency, converter)
    private var iterator = if (!stream.isEmpty) stream.head.iterator else null
    //public api for the iterator
    def next : Row[T] = {
      if (hasNext) {
        return iterator.next
      }
      throw new NoSuchElementException("8====D")
    }
    
    def hasNext : Boolean = {
      if (stream.isEmpty) { return false }
      if (null != iterator) {
        if (iterator.hasNext) {
          true
        } else {
          stream = stream.tail
          if (!stream.isEmpty) iterator = stream.head.iterator
          hasNext
        }
      } else {
        false
      }
    }
  }
}

class GetColumnScanner(ks : Keyspace, 
                       cp : thrift.ColumnParent, 
                       predicate : thrift.SlicePredicate, 
                       range : thrift.KeyRange,
                       consistency : ReadConsistency) extends Scanner[GetColumn](ks, cp, predicate, range, consistency) {
  
  def converter = {
    (container : thrift.ColumnOrSuperColumn) => new GetColumn(container.column)
  }
}

class GetSuperColumnScanner(ks : Keyspace,
                            cp : thrift.ColumnParent,
                            predicate : thrift.SlicePredicate,
                            range : thrift.KeyRange,
                            consistency : ReadConsistency) extends Scanner[GetSuperColumn](ks, cp, predicate, range, consistency) {
  def converter = {
    (container : thrift.ColumnOrSuperColumn) => new GetSuperColumn(container.super_column)
  }
}

object ByteArrayOrdering extends Ordering[Array[Byte]] {
  def compare(a : Array[Byte], b : Array[Byte]) : Int = {
    var index = 0
    while (index < a.length && index < a.length) {
      val comp = a(index) compare b(index)
      if (comp != 0) return comp
      index += 1
    }
    if (a.length < b.length) return -1
    if (a.length > b.length) return 1
    return 0
  }
}

object RowOrdering extends Ordering[Row[_]] {
  def compare(a : Row[_], b : Row[_]) : Int = {
    val bytesA = a.key.getBytes
    val bytesB = b.key.getBytes
    ByteArrayOrdering.compare(bytesA, bytesB)
  }
}

object ScanStream extends Log {
  def apply[T](ks : Keyspace,
            cp : thrift.ColumnParent,
            predicate : thrift.SlicePredicate,
            range : thrift.KeyRange,
            consistency : ReadConsistency,
            converter : thrift.ColumnOrSuperColumn => T) : Stream[Seq[Row[T]]] = {
    ks.pool.withConnection { conn =>
      if (ByteArrayOrdering.compare(range.start_key.getBytes,range.end_key.getBytes) >= 0) {
        Stream.empty
      } else {
        val results = conn.get_range_slices(ks.name,
          cp,
          predicate,
          range,
          consistency.thrift)
        debug("get_range_slices(" + 
          ks.name + ", " + 
          cp + ", " + 
          predicate + ", " + 
          "KeyRange(start_key:" + rubyStringHex(range.start_key.getBytes) + ",end_key:" + rubyStringHex(range.end_key.getBytes) + ")" + ", " + 
          consistency.thrift + ")")
        
        val rows = results.map { slice =>
          Row(slice.key, slice.columns.map(converter(_)))
        }.sorted(RowOrdering)
        
        if (rows.length == 0) {
          Stream.empty
        } else {
          val newRange = updateRange(range, rows)
          if (null == newRange) {
            Stream.cons(rows, Stream.empty)
          } else {
            Stream.cons(rows, apply(ks, cp, predicate, newRange, consistency, converter))
          }
        }
      }
    }
  }
  
  def updateRange[T](r : thrift.KeyRange, rows : Seq[Row[T]]) : thrift.KeyRange = {
    val range = r.deepCopy
    val lastKey = rows.last.key
    val nextStart = scromium.util.Roll.roll(lastKey)
    if (nextStart == range.start_key) return null
    if (range.start_key == null) {
      range.start_token = lastKey
    } else {
      range.start_key = nextStart
    }
    range
  }
  
  private def rubyStringHex(ary : Seq[Byte]) : String = {
    val buff = new StringBuilder
    for (b <- ary) {
      if (b >= 33 && b <= 126) {
        buff += b.toChar
      } else {
        var str = (b.toLong & 0xff).toOctalString
        if (str.length == 2) {
          str = "0" + str
        } else if (str.length == 1) {
          str = "00" + str
        }
        buff ++= ("\\" + str)
      }
    }
    buff.toString
  }
}