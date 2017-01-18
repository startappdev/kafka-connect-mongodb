package com.startapp.data

import java.util

import com.mongodb.casbah.Imports._
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.connect.sink.{SinkRecord, SinkTask}

import scala.collection.mutable.ListBuffer
import scala.collection.JavaConversions._

/**
  * Created by Raz on 05/01/2017.
  */
class MongoSinkTask extends SinkTask{
  private var mongoConnection : MongoConnection = _
  private var collections : Map[String,MongoCollection] = _
  private var topicToRecords : Map[String, ListBuffer[MongoDBObject]] = _
  private var connectMongoConverter : ConnectMongoConverter = _
  private var config: MongoSinkConfig = _
  //private var totalRecords = 0

  override def start(props: util.Map[String, String]): Unit = {
    config = MongoSinkConfig(props)
    mongoConnection = MongoConnection(config.hostName,config.portNum)

    collections = config.topicToCollection.map(t=> (t._1, mongoConnection(config.dbName)(t._2)))
    topicToRecords = config.topics.map((_, ListBuffer.empty[MongoDBObject])).toMap

    connectMongoConverter = if(config.useSchema) {
      SchemaConnectMongoConverter
    }
    else {
      NoSchemaConnectMongoConverter
    }
  }

  private def writeTopic(topic : String): Unit = {
    if(topicToRecords(topic).nonEmpty){
      //val t0 = Calendar.getInstance().getTimeInMillis

      val bulk = collections(topic).initializeUnorderedBulkOperation

      topicToRecords(topic).foreach{dbObj =>
        if(config.recordKeys != null && config.recordFields != null){
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

      //val t1 = Calendar.getInstance().getTimeInMillis
      bulk.execute(WriteConcern.Unacknowledged)
      //val t2 = Calendar.getInstance().getTimeInMillis

      //println(s"delta time until execute: ${t1-t0}. delta time for execute: ${t2-t1}. delta time for all: ${t2-t0}. records size: ${topicToRecords(topic).length}")

      topicToRecords(topic).clear()
    }
  }

  override def put(records: util.Collection[SinkRecord]): Unit = {
    if(records.size() > 0){
      //totalRecords += records.size()
      //println(s"RECORD SIZE: ${records.size()} TOTAL RECORDS: $totalRecords")

      for (record <- records) {
        val topic = record.topic()
        val jsonMap = connectMongoConverter.toJsonMap(record.value()).map{v=>
          (if(config.recordRenamerMap.containsField(v._1)) {config.recordRenamerMap(v._1)} else {v._1}, v._2)
        }

        topicToRecords(topic) += MongoDBObject(jsonMap)

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