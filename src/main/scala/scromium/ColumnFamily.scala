package scromium

import serializers._
import client.ClientProvider
import scromium.util.Log
import clocks._

class ColumnFamily(ksName : String,
    cfName : String,
    provider : ClientProvider,
    defaultR : ReadConsistency,
    defaultW : WriteConsistency,
    defaultClock : Clock) {
    
  def selector[R](row : R)(implicit ser : Serializer[R]) = 
    new Selector(List(ser.serialize(row)))
    
  def selector[R](rows : List[R])(implicit ser : Serializer[R]) =
    new Selector(rows.map(ser.serialize(_)))
    
  def batch(clock : Clock = defaultClock) = new Put(clock)
    
  def get(selector : Selector, consistency : ReadConsistency = defaultR) : RowIterator[Column] = {
    null
  }
  
  def delete(selector : Selector, clock : Clock = defaultClock, consistency : WriteConsistency = defaultW) {
    
  }
  
  def put(put : Put, clock : Clock = defaultClock, consistency : WriteConsistency = defaultW) {
    
  }
}

class SuperColumnFamily(ksName : String,
    cfName : String,
    provider : ClientProvider,
    defaultR : ReadConsistency,
    defaultW : WriteConsistency,
    defaultClock : Clock) {

  def selector[R](row : R)(implicit ser : Serializer[R]) = 
    new SuperSelector(List(ser.serialize(row)))

  def selector[R](rows : List[R])(implicit ser : Serializer[R]) =
    new SuperSelector(rows.map(ser.serialize(_)))
    
  def batch(clock : Clock = defaultClock) = new SuperPut(clock)

  def get(selector : SuperSelector, consistency : ReadConsistency = defaultR) : RowIterator[Column] = {
    null
  }

  def delete(selector : SuperSelector, clock : Clock = defaultClock, consistency : WriteConsistency = defaultW) {
    
  }

  def put(put : SuperPut, clock : Clock = defaultClock, consistency : WriteConsistency = defaultW) {
    
  }
}