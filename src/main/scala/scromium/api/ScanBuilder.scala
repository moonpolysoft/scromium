package scromium.api

import org.apache.cassandra.thrift
import scromium._
import serializers._

class ColumnScanBuilder(val ks : Keyspace, val cf : String) extends QueryBuilder {
  val cp = new thrift.ColumnParent
  cp.column_family = cf
  
  def this(ks : Keyspace, cf : String, superColumn : Array[Byte]) {
    this(ks, cf)
    cp.super_column = superColumn
  }
  
  def !(implicit consistency : ReadConsistency) : GetColumnScanner = {
    new GetColumnScanner(ks, cp, predicate, range, consistency)
  }
}

class SuperColumnScanBuilder(val ks : Keyspace, cf : String) extends QueryBuilder {
  val cp = new thrift.ColumnParent
  cp.column_family = cf
  
  def !(implicit consistency : ReadConsistency) : GetSuperColumnScanner = {
    new GetSuperColumnScanner(ks, cp, predicate, range, consistency)
  }
}