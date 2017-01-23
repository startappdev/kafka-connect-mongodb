package com.startapp.data

import java.util

import org.apache.kafka.connect.connector.Task
import org.apache.kafka.connect.sink.SinkConnector

/**
  * MongoSinkConnector is a Kafka Connect Sink connector that
  * consumes topic messages and stores them in a MongoDB collection.
  * For more information, check: https://github.com/startappdev/kafka-connect-mongodb
  */
class MongoSinkConnector extends SinkConnector {
  private var taskConfigs: util.Map[String, String] = _

  override def start(props: util.Map[String, String]): Unit = {
    taskConfigs = props
  }

  override def stop(): Unit = {}

  override def taskClass(): Class[_ <: Task] = classOf[MongoSinkTask]

  override def taskConfigs(maxTasks: Int): util.List[util.Map[String, String]] = {
    util.Collections.nCopies(maxTasks, taskConfigs)
  }

  override def version(): String = getClass.getPackage.getImplementationVersion

}
