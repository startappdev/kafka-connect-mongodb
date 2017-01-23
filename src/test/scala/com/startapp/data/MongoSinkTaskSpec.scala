package com.startapp.data

import java.lang.Boolean

import com.mongodb.casbah.BulkWriteOperation
import org.apache.kafka.connect.data.{Schema, SchemaBuilder, Struct}
import org.apache.kafka.connect.data.Schema.Type
import org.apache.kafka.connect.sink.SinkRecord
import org.mockito.Mockito
import org.scalatest.mockito.MockitoSugar
import org.scalatest.{FlatSpec, Matchers}
import org.mockito.ArgumentMatchers._
import sun.reflect.generics.reflectiveObjects.NotImplementedException


/**
  * Created by Raz on 23/01/2017.
  */
class MongoSinkTaskSpec extends FlatSpec with Matchers with MockitoSugar {

  "empty put" should "not execute bulk" in {
    val sinkTask = Mockito.spy(new MongoSinkTask())
    Mockito.doNothing().when(sinkTask).executeBulkOperation(any[BulkWriteOperation])

    val configs = MongoSinkConfigSpec.mostBasicConfigs.clone().asInstanceOf[java.util.HashMap[String,String]]
    configs.put("db.collections","testCollection")
    configs.put("topics","testTopic")

    sinkTask.start(configs)

    val recordsList = new java.util.ArrayList[SinkRecord]()
    sinkTask.put(recordsList)

    Mockito.verify(sinkTask, Mockito.times(0)).executeBulkOperation(any[BulkWriteOperation])
  }

  "big put without batches" should "bulk execute all records once" in {
    bigPutTest(null, 10000, false){confs =>
      confs.put("connect.use_schema","false")
    }

    val valueSchema = SchemaBuilder.struct().name("test schema")
      .field("f1", Schema.INT32_SCHEMA)
      .field("f2", Schema.STRING_SCHEMA)
      .field("f3", Schema.BOOLEAN_SCHEMA)
      .build()

    bigPutTest(valueSchema, 10000, false){confs =>
      confs.put("connect.use_schema","true")
    }
  }

  "big put with batches" should "bulk execute all records in batches" in {
    bigPutTest(null, 10000, true){confs =>
      confs.put("connect.use_schema","false")
    }

    val valueSchema = SchemaBuilder.struct().name("test schema")
      .field("f1", Schema.INT32_SCHEMA)
      .field("f2", Schema.STRING_SCHEMA)
      .field("f3", Schema.BOOLEAN_SCHEMA)
      .build()

    bigPutTest(valueSchema, 10000, true){confs =>
      confs.put("connect.use_schema","true")
    }
  }

  "big put with keys" should "bulk execute all records in batches" in {
    bigPutTest(null, 10000, true){confs =>
      confs.put("connect.use_schema","false")
      confs.put("record.keys","f1")
    }

    val valueSchema = SchemaBuilder.struct().name("test schema")
      .field("f1", Schema.INT32_SCHEMA)
      .field("f2", Schema.STRING_SCHEMA)
      .field("f3", Schema.BOOLEAN_SCHEMA)
      .build()

    bigPutTest(valueSchema, 10000, true){confs =>
      confs.put("connect.use_schema","true")
      confs.put("record.keys","f1")
    }
  }

  "big put with renamer" should "bulk execute all records in batches" in {
    bigPutTest(null, 10000, true){confs =>
      confs.put("connect.use_schema","false")
      confs.put("record.keys","f1")
      confs.put("record.fields.rename","f1=>newF1")
    }

    val valueSchema = SchemaBuilder.struct().name("test schema")
      .field("f1", Schema.INT32_SCHEMA)
      .field("f2", Schema.STRING_SCHEMA)
      .field("f3", Schema.BOOLEAN_SCHEMA)
      .build()

    bigPutTest(valueSchema, 10000, true){confs =>
      confs.put("connect.use_schema","true")
      confs.put("record.keys","f1")
      confs.put("record.fields.rename","f1=>newF1")
    }
  }

  "big put with specific fields" should "bulk execute all records in batches" in {
    bigPutTest(null, 10000, true){confs =>
      confs.put("connect.use_schema","false")
      confs.put("record.keys","f1")
      confs.put("record.fields.rename","f1=>newF1")
      confs.put("record.fields","Id")
    }

    val valueSchema = SchemaBuilder.struct().name("test schema")
      .field("f1", Schema.INT32_SCHEMA)
      .field("f2", Schema.STRING_SCHEMA)
      .field("f3", Schema.BOOLEAN_SCHEMA)
      .build()

    bigPutTest(valueSchema, 10000, true){confs =>
      confs.put("connect.use_schema","true")
      confs.put("record.keys","f1")
      confs.put("record.fields.rename","f1=>newF1")
      confs.put("record.fields","Id")
    }
  }

  def bigPutTest(valueSchema: Schema, recordsNum: Int, useBatches: Boolean)(addConfs: java.util.HashMap[String,String] => Unit): Unit ={
    val sinkTask = Mockito.spy(new MongoSinkTask())
    Mockito.doNothing().when(sinkTask).executeBulkOperation(any[BulkWriteOperation])

    val configs = MongoSinkConfigSpec.mostBasicConfigs.clone().asInstanceOf[java.util.HashMap[String,String]]
    configs.put("db.collections","testCollection")
    configs.put("topics","testTopic")
    if(useBatches){
      configs.put("write.batch.enabled","true")
      configs.put("write.batch.size","200")
    }
    addConfs(configs)

    val recordsList = new java.util.ArrayList[SinkRecord]()
    (1 to recordsNum).foreach{i =>
      recordsList.add(createRandomSinkRecord("testTopic", 5, valueSchema, List("f1","f2","f3"), i))
    }

    sinkTask.start(configs)

    sinkTask.put(recordsList)
    val times = recordsNum/200 + (if(recordsNum % 200 == 0) 0 else 1)
    Mockito.verify(sinkTask, Mockito.times(if(useBatches) times else 1)).executeBulkOperation(any[BulkWriteOperation])
  }

  private def createRandomSinkRecord(topic: String, largestPartition: Int, valueSchema: Schema, fields: List[String], offset: Int) : SinkRecord = {
    val r = scala.util.Random

    val partition = r.nextInt(largestPartition)
    val value = new java.util.HashMap[String,Object]()
    var valueStruct = if (valueSchema != null) new Struct(valueSchema) else null
    fields.foreach{f=>
      if(valueSchema == null){
        value.put(f, r.nextString(10))
      }
      else{
        val o = valueSchema.field(f).schema().`type`() match {
          case Type.INT32 => new Integer(r.nextInt(50))
          case Type.BOOLEAN => new Boolean(r.nextBoolean())
          case Type.STRING => r.nextString(50)
          case _ => throw new NotImplementedException()
        }
        valueStruct = valueStruct.put(f, o)
      }
    }

    new SinkRecord(topic,partition,null,null,valueSchema,if(valueSchema == null) value else valueStruct,offset)
  }
}
