package scromium

import serializers._
import serializers.Serializers.ByteArraySerializer
import client.ClientProvider
import scromium.util.Log
import clocks._

class ColumnFamily(ksName : String,
    cfName : String,
    provider : ClientProvider,
    defaultR : ReadConsistency,
    defaultW : WriteConsistency,
    defaultClock : Clock) {
      
  def apply[T](f : ColumnFamily => T) = f(this)
    
  def selector[R](row : R)(implicit ser : Serializer[R]) = 
    new Selector(List(ser.serialize(row)))
    
  def selector[R](rows : List[R])(implicit ser : Serializer[R]) =
    new Selector(rows.map(ser.serialize(_)))
    
  def batch = new Put(defaultClock)
  def batch(clock : Clock) = new Put(clock)
    
  def getColumn[R,C](row : R, column : C, consistency : ReadConsistency = defaultR)
      (implicit rSer : Serializer[R], cSer : Serializer[C]) : Option[Column] = {
    val selector = new Selector(List(rSer.serialize(row))).column(cSer.serialize(column))
    val results = get(selector, consistency)
    for (row <- results; column <- row.columns) return Some(column)
    return None
  }
    
  def get(selector : Readable, consistency : ReadConsistency = defaultR) : RowIterator[Column] = {
    provider.withClient(_.get(ksName, selector.toRead(cfName), consistency))
  }
  
  def deleteColumn[R,C](row : R, column : C, clock : Clock = defaultClock, consistency : WriteConsistency = defaultW)
      (implicit rSer : Serializer[R], cSer : Serializer[C]) = {
    val selector = new Selector(List(rSer.serialize(row))).column(cSer.serialize(column))
    delete(selector, clock, consistency)
  }
  
  def delete(selector : Deletable, clock : Clock = defaultClock, consistency : WriteConsistency = defaultW) {
    provider.withClient(_.delete(ksName, selector.toDelete(cfName, clock), consistency))
  }
  
  def putColumn[R,C,V](row : R, column : C, value : V, clock : Clock = defaultClock, consistency : WriteConsistency = defaultW)
      (implicit rSer : Serializer[R], cSer : Serializer[C], vSer : Serializer[V]) {
    val put = batch(clock)
    put.row(row)(rSer).insert(column, value)(cSer,vSer)
    this.put(put, consistency)
  }
  
  def put(put : Put, consistency : WriteConsistency = defaultW) {
    provider.withClient(_.put(ksName, put.toWrites(cfName), consistency))
  }
}

class SuperColumnFamily(ksName : String,
    cfName : String,
    provider : ClientProvider,
    defaultR : ReadConsistency,
    defaultW : WriteConsistency,
    defaultClock : Clock) {

  def apply[T](f : SuperColumnFamily => T) = f(this)

  def selector[R](row : R)(implicit ser : Serializer[R]) = 
    new SuperSelector(List(ser.serialize(row)))

  def selector[R](rows : List[R])(implicit ser : Serializer[R]) =
    new SuperSelector(rows.map(ser.serialize(_)))
    
  def batch = new SuperPut(defaultClock)
  def batch(clock : Clock) = new SuperPut(clock)

  def getSuperColumn[R,S](row : R, sc : S, consistency : ReadConsistency = defaultR)
      (implicit rSer : Serializer[R], scSer : Serializer[S]) : Option[SuperColumn] = {
    val selector = new SuperSelector(List(rSer.serialize(row))).
      superColumn(scSer.serialize(sc))
    val results = get(selector, consistency)
    for (row <- results; column <- row.columns) return Some(column)
    return None
  }

  def getSubColumn[R,S,C](row : R, sc : S, c : C, consistency : ReadConsistency = defaultR)
      (implicit rSer : Serializer[R], scSer : Serializer[S], cSer : Serializer[C]) : Option[Column] = {
    val selector = new SuperSelector(List(rSer.serialize(row))).
      superColumn(scSer.serialize(sc)).
      subColumn(cSer.serialize(c))
    val results = get(selector, consistency)
    for (row <- results; c <- row.columns) return Some(c)
    return None
  }  

  def get(selector : SuperReadable) : RowIterator[SuperColumn] = get(selector, defaultR)
  def get(selector : SuperReadable, consistency : ReadConsistency) : RowIterator[SuperColumn] = {
    provider.withClient(_.superGet(ksName, selector.toRead(cfName), consistency))
  }

  def get(selector : Readable) : RowIterator[Column] = get(selector, defaultR)
  def get(selector : Readable, consistency : ReadConsistency) : RowIterator[Column] = {
    provider.withClient(_.get(ksName, selector.toRead(cfName), consistency))
  }
  
  def deleteSuperColumn[R,S](row : R, sc : S, clock : Clock = defaultClock, consistency : WriteConsistency = defaultW)
      (implicit rSer : Serializer[R], scSer : Serializer[S]) {
    val selector = new SuperSelector(List(rSer.serialize(row))).
      superColumn(scSer.serialize(sc))
    provider.withClient(_.delete(ksName, selector.toDelete(cfName, clock), consistency))
  }
  
  def deleteSubColumn[R,S,C](row : R, sc : S, c : C, clock : Clock = defaultClock, consistency : WriteConsistency = defaultW)
      (implicit rSer : Serializer[R], scSer : Serializer[S], cSer : Serializer[C]) {
    val selector = new SuperSelector(List(rSer.serialize(row))).
      superColumn(scSer.serialize(sc)).
      subColumn(cSer.serialize(c))
    provider.withClient(_.delete(ksName, selector.toDelete(cfName, clock), consistency))
  }
  
  def delete(selector : Deletable, clock : Clock = defaultClock, consistency : WriteConsistency = defaultW) {
    provider.withClient(_.delete(ksName, selector.toDelete(cfName, clock), consistency))
  }

  def putSubColumn[R,S,C,V](row : R, sc : S, c : C, value : V, clock : Clock = defaultClock, consistency : WriteConsistency = defaultW)
      (implicit rSer : Serializer[R], scSer : Serializer[S], cSer : Serializer[C], vSer : Serializer[V]) {
    val put = batch(clock)
    put.row(row)(rSer).superColumn(sc)(scSer).insert(c, value)(cSer, vSer)
    this.put(put, consistency)
  }

  def put(put : SuperPut, consistency : WriteConsistency = defaultW) {
    provider.withClient(_.superPut(ksName, put.toWrites(cfName), consistency))
  }
}