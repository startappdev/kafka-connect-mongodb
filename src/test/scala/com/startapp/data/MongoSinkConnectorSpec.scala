package com.startapp.data

import org.scalatest.{FlatSpec, Matchers}

/**
  * Created by Raz on 23/01/2017.
  */
class MongoSinkConnectorSpec extends FlatSpec with Matchers {
  "connector" should "create max tasks configs for class: MongoSincTask" in {
    val configs = new java.util.HashMap[String,String]()
    val connector = new MongoSinkConnector()
    connector.start(configs)

    val taskClass = connector.taskClass()
    val tasksConfList = connector.taskConfigs(5)
    val version = connector.getClass.getPackage.getImplementationVersion

    taskClass should be(classOf[MongoSinkTask])
    tasksConfList.size() should be(5)
    connector.version() should be(version)
  }
}
