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
      val bulk = collections(topic).initializeUnorderedBulkOperation
      topicToRecords(topic).foreach(bulk.insert(_))
      bulk.execute(WriteConcern.Unacknowledged)

      topicToRecords(topic).clear()
    }
  }

  override def put(records: util.Collection[SinkRecord]): Unit = {
    if(records.size() > 0){
      for (record <- records) {
        val topic = record.topic()
        val jsonMap = connectMongoConverter.toJsonMap(record.value())

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