package com.startapp.data

import java.util

import org.apache.kafka.common.config.{AbstractConfig, ConfigDef}
import scala.collection.JavaConversions._
/**
  * Created by Raz on 11/01/2017.
  */
class MongoSinkConfig(props: java.util.Map[_,_]) extends AbstractConfig(MongoSinkConfig.configDef, props) {
  val hostName: String = getString(MongoSinkConfig.DB_HOST)
  val portNum: Integer = getInt(MongoSinkConfig.DB_PORT)
  val dbName: String = getString(MongoSinkConfig.DB_NAME)
  val useBatches: Boolean = getBoolean(MongoSinkConfig.WRITE_BATCH_ENABLED)
  val batchSize : Integer = getInt(MongoSinkConfig.WRITE_BATCH_SIZE)
  val useSchema: Boolean = getBoolean(MongoSinkConfig.USE_SCHEMA)
  val topics: List[String] = getList(MongoSinkConfig.TOPICS).toList
  val topicToCollection: Map[String, String] = getList(MongoSinkConfig.DB_COLLECTIONS).zipWithIndex.map(t=> (topics(t._2), t._1)).toMap

  println(topicToCollection)
}

object MongoSinkConfig {
  def apply(props: util.Map[_, _]): MongoSinkConfig = new MongoSinkConfig(props)

  val DB_HOST = "db.host"
  val DB_HOST_DEFAULT = "localhost"
  val DB_HOST_DOC = "The DB host name"

  val DB_PORT = "db.port"
  val DB_PORT_DEFAULT = 27017
  val DB_PORT_DOC = "The DB port number"

  val DB_NAME = "db.name"
  val DB_NAME_DEFAULT = ""
  val DB_NAME_DOC = "The Mongo Database name"

  val DB_COLLECTIONS = "db.collections"
  val DB_COLLECTIONS_DOC = "The DB collections"

  val WRITE_BATCH_ENABLED = "write.batch.enabled"
  val WRITE_BATCH_ENABLED_DEFAULT = false
  val WRITE_BATCH_ENABLED_DOC = "Enable/Disable batch writing"

  val WRITE_BATCH_SIZE = "write.batch.size"
  val WRITE_BATCH_SIZE_DEFAULT: Int = 200
  val WRITE_BATCH_SIZE_DOC = "Max records batch size for writing."

  val USE_SCHEMA = "connect.use_schema"
  val USE_SCHEMA_DEFAULT = true
  val USE_SCHEMA_DOC = "Schema based data (true/false)"

  val TOPICS = "topics"
  val TOPICS_DOC = "topics doc"



  val configDef: ConfigDef = new ConfigDef()
    .define(DB_HOST,ConfigDef.Type.STRING,DB_HOST_DEFAULT,ConfigDef.Importance.MEDIUM, DB_HOST_DOC)
    .define(DB_PORT, ConfigDef.Type.INT,DB_PORT_DEFAULT, ConfigDef.Range.between(0,65535), ConfigDef.Importance.LOW, DB_HOST_DOC)
    .define(DB_NAME, ConfigDef.Type.STRING,DB_NAME_DEFAULT, ConfigDef.Importance.HIGH,DB_NAME_DOC)
    .define(DB_COLLECTIONS, ConfigDef.Type.LIST,ConfigDef.Importance.HIGH, DB_COLLECTIONS)
    .define(WRITE_BATCH_ENABLED,ConfigDef.Type.BOOLEAN, WRITE_BATCH_ENABLED_DEFAULT,ConfigDef.Importance.MEDIUM, WRITE_BATCH_ENABLED_DOC)
    .define(WRITE_BATCH_SIZE, ConfigDef.Type.INT,WRITE_BATCH_SIZE_DEFAULT, ConfigDef.Range.atLeast(1), ConfigDef.Importance.MEDIUM, WRITE_BATCH_SIZE_DOC)
    .define(USE_SCHEMA,ConfigDef.Type.BOOLEAN, USE_SCHEMA_DEFAULT,ConfigDef.Importance.HIGH, USE_SCHEMA_DOC)
    .define(TOPICS, ConfigDef.Type.LIST,ConfigDef.Importance.HIGH, TOPICS_DOC)
}
