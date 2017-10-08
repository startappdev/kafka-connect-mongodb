package com.startapp.data

import java.util

import com.startapp.data.validators.RenameListValidator
import org.apache.kafka.common.config.{AbstractConfig, ConfigDef, ConfigException}

import scala.collection.JavaConversions._
import scala.util.matching.Regex

/**
  * MongoSinkConfig defines the configurations with validations of the connector
  */
class MongoSinkConfig(props: java.util.Map[_,_]) extends AbstractConfig(MongoSinkConfig.configDef, props) {
  val hostName: String = getString(MongoSinkConfig.DB_HOST)
  val portNum: Integer = getInt(MongoSinkConfig.DB_PORT)
  val dbName: String = getString(MongoSinkConfig.DB_NAME)

  val useBatches: Boolean = getBoolean(MongoSinkConfig.WRITE_BATCH_ENABLED)
  val batchSize : Integer = getInt(MongoSinkConfig.WRITE_BATCH_SIZE)
  val useSchema: Boolean = getBoolean(MongoSinkConfig.USE_SCHEMA)

  val topics: List[String] = getList(MongoSinkConfig.TOPICS).toList

  val recordKeys: List[String] = if(getList(MongoSinkConfig.RECORD_KEYS) != null) {
      getList(MongoSinkConfig.RECORD_KEYS).toList
    }
    else{
      null
    }
  val incremetFields: List[String] = if(getList(MongoSinkConfig.RECORD_INCREMENT) != null){
    getList(MongoSinkConfig.RECORD_INCREMENT).toList
  }
  else{
    null
  }

  val recordFields: List[String] = if(getList(MongoSinkConfig.RECORD_FIELDS) != null){
      getList(MongoSinkConfig.RECORD_FIELDS).toList
    }
    else{
      null
    }
  val recordRenamerMap: Map[String,String] = if(getList(MongoSinkConfig.RECORD_FIELDS_RENAME)!=null){
      getList(MongoSinkConfig.RECORD_FIELDS_RENAME).toList.map(v=>(v.split("=>")(0), v.split("=>")(1))).toMap
    }
    else{
      null
    }

  val insertionTsName : String = getString(MongoSinkConfig.RECORD_INSERTION_TIME_NAME)

  val filterKey : String = getString(MongoSinkConfig.RECORD_FILTER_KEY)
  val filterRegex: Regex =  if (getString(MongoSinkConfig.RECORD_FILTER_REGEX) == null) {
    null
  }else {
    getString(MongoSinkConfig.RECORD_FILTER_REGEX).r
  }

  val topicToCollection: Map[String, String] = try {
      getList(MongoSinkConfig.DB_COLLECTIONS).zipWithIndex.map(t=> (topics(t._2), t._1)).toMap
    }
    catch {
      case e: IndexOutOfBoundsException => throw new ConfigException(e.getMessage)
    }
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

  val RECORD_KEYS = "record.keys"
  val RECORD_KEYS_DEFAULT = null
  val RECORD_KEYS_DOC = "key of the record in the db. to find the row in the db for update"

  val RECORD_INCREMENT = "record.increment.fields"
  val RECORD_INCREMENT_DEFAULT = null
  val RECORD_INCREMENT_DOC = "fields of each record in the collection to insert and increment"

  val RECORD_FIELDS = "record.fields"
  val RECORD_FIELDS_DEFAULT = null
  val RECORD_FIELDS_DOC = "fields of each record in the collection"

  val RECORD_FIELDS_RENAME = "record.fields.rename"
  val RECORD_FIELDS_RENAME_DEFAULT = null
  val RECORD_FIELDS_RENAME_DOC = "rename fields key by map. pattern: A=>B,C=>D,...,Y=>Z"

  val RECORD_INSERTION_TIME_NAME = "record.timestamp.name"
  val RECORD_INSERTION_TIME_NAME_DEFAULT = null
  val RECORD_INSERTION_TIME_NAME_DOC = "add a field of the insertion time to the collection, if record.timestamp.name is null the field will not be in the record"

  val RECORD_FILTER_KEY = "record.filter.key"
  val RECORD_FILTER_KEY_DEFAULT = null
  val RECORD_FILTER_KEY_DOC = "filter records by this key"

  val RECORD_FILTER_REGEX = "record.filter.regex"
  val RECORD_FILTER_REGEX_DEFAULT = null
  val RECORD_FILTER_REGEX_DOC = "filter records using this regex"

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
    .define(RECORD_KEYS, ConfigDef.Type.LIST,RECORD_KEYS_DEFAULT,ConfigDef.Importance.MEDIUM, RECORD_KEYS_DOC)
    .define(RECORD_INCREMENT, ConfigDef.Type.LIST,RECORD_INCREMENT_DEFAULT,ConfigDef.Importance.MEDIUM, RECORD_INCREMENT_DOC)
    .define(RECORD_FIELDS, ConfigDef.Type.LIST,RECORD_FIELDS_DEFAULT,ConfigDef.Importance.MEDIUM, RECORD_FIELDS_DOC)
    .define(RECORD_FIELDS_RENAME, ConfigDef.Type.LIST,RECORD_FIELDS_RENAME_DEFAULT, RenameListValidator,ConfigDef.Importance.LOW, RECORD_FIELDS_RENAME_DOC)
    .define(RECORD_INSERTION_TIME_NAME, ConfigDef.Type.STRING, RECORD_INSERTION_TIME_NAME_DEFAULT, ConfigDef.Importance.MEDIUM, RECORD_INSERTION_TIME_NAME_DOC)
    .define(RECORD_FILTER_KEY, ConfigDef.Type.STRING, RECORD_FILTER_KEY_DEFAULT, ConfigDef.Importance.LOW, RECORD_FILTER_KEY_DOC)
    .define(RECORD_FILTER_REGEX, ConfigDef.Type.STRING, RECORD_FILTER_REGEX_DEFAULT, ConfigDef.Importance.LOW, RECORD_FILTER_REGEX_DOC)
    .define(TOPICS, ConfigDef.Type.LIST,ConfigDef.Importance.HIGH, TOPICS_DOC)
}
