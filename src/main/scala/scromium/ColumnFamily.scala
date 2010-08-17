package scromium

import serializers._
import client.ClientProvider
import scromium.util.Log

class ColumnFamily(ksName : String,
    cfName : String,
    provider : ClientProvider,
    defaultR : ReadConsistency,
    defaultW : WriteConsistency) {
    
  def put[K,N,V](key : K, name : N, value : V)(
    implicit kSer : Serializer[K],
             nSer : Serializer[N],
             vSer : Serializer[V]) {
    put(key,name,value,System.currentTimeMillis)(kSer,nSer,vSer)
  }
  
  def put[K,N,V](key : K, name : N, value : V, timestamp : Long)(
    implicit kSer : Serializer[K],
             nSer : Serializer[N],
             vSer : Serializer[V]) {
    
  }
  
  def get(cmd : Get) {
    
  }
  
  def delete(cmd : Delete) {
    
  }
  
  def scan(cmd : Scanner[Column]) : RowIterator[Column] = {
    null
  } 
}

/*def SuperColumnFamily(ksName : String,
    cfName : String,
    provider : ClientProvider,
    defaultR : ReadConsistency,
    defaultW : WriterConsistency) {
  
  def put()
  
}*/