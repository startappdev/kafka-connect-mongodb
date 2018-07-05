package com.startapp.data

import java.util

import com.mongodb.casbah.{BulkWriteOperation, query}
import com.mongodb.casbah.Imports._
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.connect.sink.{SinkRecord, SinkTask}

import scala.collection.mutable.ListBuffer
import scala.collection.JavaConversions._

/**
  * MongoSinkTask is a Kafka Connect Sink Task for the MongoSinkConnector.
  * It is responsible for inserting Kafka messages into a mongo db collection.
  * For more information, check: https://github.com/startappdev/kafka-connect-mongodb
  */
class MongoSinkTask extends SinkTask{
  private var collections : Map[String,MongoCollection] = _
  private var topicToRecords : Map[String, ListBuffer[MongoDBObject]] = _
  private var connectMongoConverter : ConnectMongoConverter = _
  private var config: MongoSinkConfig = _
  //private var totalRecords = 0

  /**
    * Start the task.
    * Responsible for establishment with MongoDb and initialize necessary information by the connector configurations.
    */
  override def start(props: util.Map[String, String]): Unit = {
    config = MongoSinkConfig(props)

    val server = new ServerAddress(config.hostName, config.portNum)
    val mongoClient = if(config.dbAuth == null){
      MongoClient(server)
    } else {
      val credentials = MongoCredential.createCredential(config.dbAuth.username, config.dbAuth.source, config.dbAuth.password.toCharArray)
      MongoClient(server, List(credentials))
    }

    collections = config.topicToCollection.map(t=> (t._1, mongoClient(config.dbName)(t._2)))
    topicToRecords = config.topics.map((_, ListBuffer.empty[MongoDBObject])).toMap

    connectMongoConverter = if(config.useSchema) {
      SchemaConnectMongoConverter
    }
    else {
      NoSchemaConnectMongoConverter
    }
  }

  /**
    * Writes topic data into the collection
    */
  private def writeTopic(topic : String): Unit = {
    if(topicToRecords(topic).nonEmpty){
      val bulk = collections(topic).initializeUnorderedBulkOperation
      topicToRecords(topic).foreach{dbObj =>
        if(config.recordKeys != null && config.incremetFields != null){
          bulk.find(dbObj.filter(field=>config.recordKeys.contains(field._1)))
            .upsert().update($inc(dbObj.filter(field=>config.incremetFields.contains(field._1)).toList.map{kv => (kv._1, kv._2.asInstanceOf[Double])} : _*))
        }
        else if(config.recordKeys != null && config.recordFields != null){
          bulk.find(dbObj.filter(field=>config.recordKeys.contains(field._1)))
            .upsert().update($set(dbObj.filter(field=>config.recordFields.contains(field._1)).toList : _*))
        }
        else if(config.recordKeys != null ){
          bulk.find(dbObj.filter(field=>config.recordKeys.contains(field._1)))
            .upsert().update($set(dbObj.toList : _*))
        }
        else if(config.recordFields != null){
          bulk.insert(dbObj.filter(field=>config.recordFields.contains(field._1)))
        }
        else { //config.recordKeys == null && config.recordFields == null
          bulk.insert(dbObj)
        }
      }

      executeBulkOperation(bulk)

      topicToRecords(topic).clear()
    }
  }

  def executeBulkOperation(bulk : BulkWriteOperation): Unit ={
    //val t0 = Calendar.getInstance().getTimeInMillis
    if(config.useMongoAck){
      bulk.execute(WriteConcern.Acknowledged)
    } else {
      bulk.execute(WriteConcern.Unacknowledged)
    }
    //val t1 = Calendar.getInstance().getTimeInMillis
    //println(s"delta time for execute: ${t1-t0}.")
  }

  override def put(records: util.Collection[SinkRecord]): Unit = {
    if(records.size() > 0){
      //totalRecords += records.size()
      //println(s"RECORD SIZE: ${records.size()} TOTAL RECORDS: $totalRecords")

      for (record <- records) {
        var insertRecord = true

        val topic = record.topic()
        val jsonMap = if (config.recordRenamerMap == null){
          connectMongoConverter.toJsonMap(record.value())
        }else{
          connectMongoConverter.toJsonMap(record.value()).map{v=>
            (if(config.recordRenamerMap.containsField(v._1)) {config.recordRenamerMap(v._1)} else {v._1}, v._2)
          }
        }

        if(config.filterKey != null && config.filterRegex != null){
          val filterKV = jsonMap.filter(_._1 == config.filterKey)
          if(filterKV.nonEmpty && config.filterRegex.pattern.matcher(filterKV.head._2.toString).matches)
            insertRecord = false
        }

        if(insertRecord){
          val dbObj = MongoDBObject(jsonMap)
          if(config.insertionTsName != null){
            val timestamp = new java.lang.Long(System.currentTimeMillis())
            dbObj.put(config.insertionTsName, timestamp)
          }

          topicToRecords(topic) += dbObj
        }

        if(config.useBatches){

          if(topicToRecords(topic).length == config.batchSize){
            writeTopic(topic)
          }
        }
      }

      topicToRecords.keys.foreach(writeTopic)

    }
  }

  override def version(): String = getClass.getPackage.getImplementationVersion

  override def stop(): Unit = {}

  override def flush(offsets: util.Map[TopicPartition, OffsetAndMetadata]): Unit = {}

}