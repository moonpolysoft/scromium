package scromium

import serializers._
import client.ClientProvider
import scromium.util.Log
import clocks._

class Keyspace(val name : String, val provider : ClientProvider) extends Log {
  
  def apply[T](f : Keyspace => T) = f(this)
  
  def columnFamily(cfName : String, 
    defaultReadConsistency : ReadConsistency = ReadConsistency.Quorum, 
    defaultWriteConsistency : WriteConsistency = WriteConsistency.Quorum,
    defaultClock : Clock = MicrosecondEpochClock) = 
      new ColumnFamily(name, 
        cfName, 
        provider, 
        defaultReadConsistency, 
        defaultWriteConsistency,
        defaultClock)
        
  def superColumnFamily(cfName : String,
    defaultReadConsistency : ReadConsistency = ReadConsistency.Quorum,
    defaultWriteConsistency : WriteConsistency = WriteConsistency.Quorum,
    defaultClock : Clock = MicrosecondEpochClock) =
      new SuperColumnFamily(name,
        cfName,
        provider,
        defaultReadConsistency,
        defaultWriteConsistency,
        defaultClock)
}
