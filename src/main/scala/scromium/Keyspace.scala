package scromium

import serializers._
import client.ClientProvider
import scromium.util.Log

class Keyspace(val name : String, val provider : ClientProvider) extends Log {
  
  def columnFamily(cfName : String, 
    defaultReadConsistency : ReadConsistency = ReadConsistency.Quorum, 
    defaultWriteConsistency : WriteConsistency = WriteConsistency.Quorum) = 
      new ColumnFamily(name, cfName, 
        provider, defaultReadConsistency, defaultWriteConsistency)
}
