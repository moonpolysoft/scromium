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

case class Row[T](val key : String, val columns : Seq[T])

object ScanStream {
  def apply[T](ks : Keyspace,
            cp : thrift.ColumnParent,
            predicate : thrift.SlicePredicate,
            range : thrift.KeyRange,
            consistency : ReadConsistency,
            converter : thrift.ColumnOrSuperColumn => T) : Stream[Seq[Row[T]]] = {
    ks.pool.withConnection { conn =>
      val results = conn.client.get_range_slices(ks.name,
        cp,
        predicate,
        range,
        consistency.thrift)
        
      val rows = results.map { slice =>
        Row(slice.key, slice.columns.map(converter(_)))
      }
      
      if (rows.length == 0) {
        Stream.empty
      } else {
        Stream.cons(rows, apply(ks, cp, predicate, updateRange(range, rows), consistency, converter))
      }
    }
  }
  
  def updateRange[T](r : thrift.KeyRange, rows : Seq[Row[T]]) : thrift.KeyRange = {
    val range = r.deepCopy
    val lastKey = rows.last.key
    val nextStart = scromium.util.Roll.roll(lastKey)
    if (range.start_key == null) {
      range.start_token = lastKey
    } else {
      range.start_key = nextStart
    }
    range
  }
}