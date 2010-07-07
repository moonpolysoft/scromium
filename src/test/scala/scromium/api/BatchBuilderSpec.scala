package scromium.api

import org.specs._
import org.specs.mock.Mockito
import org.mockito.Matchers._
import org.apache.cassandra.thrift
import scromium._
import connection._
import serializers._
import Serializers._
import java.util.HashMap
import java.util.ArrayList
import scromium.util.Thrift._

class BatchBuilderSpec extends Specification with Mockito with TestHelper {
  "BatchBuilder" should {
    "execute a batch_insert with a super column" in {
      val (cassandra,client) = clientSetup
      val timestamp = Clock.timestamp
      val map = batchMap("row", "cf")
      map.get("row").get("cf").add(superColumnMutation("sc".getBytes, ("c".getBytes, "value".getBytes, timestamp)))
      
      cassandra.keyspace("ks") {ks =>
        implicit val consistency = WriteConsistency.Any
        val batch = ks.batch
        batch.row("row") { row =>
          row.add("cf" -> "sc" -> "c", "value", timestamp)
        }
        batch!
      }
      
      there was one(client).batch_mutate("ks", map, WriteConsistency.Any.thrift)
    }
    
    "execute a batch_insert with a single column" in {
      val (cassandra,client) = clientSetup
      val timestamp = Clock.timestamp
      val map = batchMap("row", "cf")
      map.get("row").get("cf").add(columnMutation("c".getBytes, "value".getBytes, timestamp))
      
      cassandra.keyspace("ks") {ks =>
        implicit val consistency = WriteConsistency.Any
        val batch = ks.batch
        batch.row("row") { row =>
          row.add("cf" -> "c", "value", timestamp)
        }
        batch!
      }
      
      there was one(client).batch_mutate("ks", map, WriteConsistency.Any.thrift)
    }
        
    "pick the right serializer" in {
      val (cassandra,client) = clientSetup
      val timestamp = Clock.timestamp
      val map = batchMap("row", "cf")
      map.get("row").get("cf").add(columnMutation("c".getBytes, Array[Byte](0), timestamp))
      
      class Fuck {}
  
      implicit object FuckSerializer extends Serializer[Fuck] {
        def serialize(fuck : Fuck) = Array[Byte](0)
    
        def deserialize(ary : Array[Byte]) = new Fuck
      }
  
      cassandra.keyspace("ks") {ks =>
        implicit val consistency = WriteConsistency.Any
        val batch = ks.batch
        batch.row("row") { row =>
          row.add("cf" -> "c", new Fuck, timestamp)
        }
        batch!
      }
  
      there was one(client).batch_mutate("ks", map, WriteConsistency.Any.thrift)
    }
        
    "pick a covariant serializer" in {
      val (cassandra,client) = clientSetup
      val timestamp = Clock.timestamp
      val map = batchMap("row", "cf")
      map.get("row").get("cf").add(columnMutation("c".getBytes, Array[Byte](0), timestamp))
      
      trait Balls
      
      class Fuck extends Balls
      
      implicit object BallsSerializer extends Serializer[Balls] {
        def serialize(fuck : Balls) = Array[Byte](0)
        
        def deserialize(ary : Array[Byte]) : Balls = new Fuck
      }
      
      cassandra.keyspace("ks") {ks =>
        implicit val consistency = WriteConsistency.Any
        val batch = ks.batch
        batch.row("row") { row =>
          row.add("cf" -> "c", new Fuck, timestamp)
        }
        batch!
      }
      
      there was one(client).batch_mutate("ks", map, WriteConsistency.Any.thrift)
    }
  }
  
  type JOpMap = java.util.Map[String, JMuteMap]
  type JMuteMap = java.util.Map[String, java.util.List[thrift.Mutation]]
  
  def batchMap(row : String, cf : String) : JOpMap = {
    val ops = new java.util.HashMap[String, java.util.Map[String, java.util.List[thrift.Mutation]]]
    val mutes = new HashMap[String, java.util.List[thrift.Mutation]]
    mutes.put(cf, new java.util.ArrayList[thrift.Mutation])
    ops.put(row, mutes)
    ops.asInstanceOf[JOpMap]
  }
}
