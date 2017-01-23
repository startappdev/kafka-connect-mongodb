package com.startapp.data

import org.apache.kafka.common.config.ConfigException
import org.scalatest.{FlatSpec, Matchers}

/**
  * Created by Raz on 22/01/2017.
  */
class MongoSinkConfigSpec extends FlatSpec with Matchers {
  "basic configurations" should "parse correctly" in {
    val parsed = MongoSinkConfig.apply(MongoSinkConfigSpec.basicGoodConfigs)

    parsed.hostName should be("localhost")
    parsed.portNum should be(27017)
    parsed.dbName should be("testdb")
    parsed.recordKeys should be(null)
    parsed.recordFields should be(null)
    parsed.recordRenamerMap should be(null)
    parsed.topicToCollection("testTopic") should be("testCollection")
  }

  "multi topics configurations" should "parse correctly" in {
    val configs = MongoSinkConfigSpec.basicConfigs.clone().asInstanceOf[java.util.HashMap[String,String]]
    configs.put("db.collections","testCollection1,testCollection2,testCollection3")
    configs.put("topics","testTopic1,testTopic2,testTopic3")

    val parsed = MongoSinkConfig.apply(configs)

    parsed.topicToCollection("testTopic1") should be("testCollection1")
    parsed.topicToCollection("testTopic2") should be("testCollection2")
    parsed.topicToCollection("testTopic3") should be("testCollection3")
  }

  "bad multi topics configurations" should "throw config exception" in {
    val configsLessTopics = MongoSinkConfigSpec.basicConfigs.clone().asInstanceOf[java.util.HashMap[String,String]]
    configsLessTopics.put("db.collections","testCollection1,testCollection2,testCollection3")
    configsLessTopics.put("topics","testTopic1,testTopic2")

    validateConfigException(configsLessTopics)

    val configsLessCollections = MongoSinkConfigSpec.basicConfigs.clone().asInstanceOf[java.util.HashMap[String,String]]
    configsLessTopics.put("db.collections","testCollection1,testCollection2")
    configsLessTopics.put("topics","testTopic1,testTopic2,testTopic3")

    validateConfigException(configsLessCollections)
  }

  "good renamer configs" should "parse correctly" in {
    val renamerConfigs1 = MongoSinkConfigSpec.basicGoodConfigs.clone().asInstanceOf[java.util.HashMap[String,String]]
    renamerConfigs1.put("record.fields.rename","ts=>timestamp")

    val parsed = MongoSinkConfig.apply(renamerConfigs1)

    parsed.recordRenamerMap("ts") should be("timestamp")

    val renamerConfigs2 = MongoSinkConfigSpec.basicGoodConfigs.clone().asInstanceOf[java.util.HashMap[String,String]]
    renamerConfigs2.put("record.fields.rename","ts=>timestamp,num=>number")

    val parsed2 = MongoSinkConfig.apply(renamerConfigs2)

    parsed2.recordRenamerMap("ts") should be("timestamp")
    parsed2.recordRenamerMap("num") should be("number")
  }

  "bad renamer configs" should "throw config exception" in {

    validateConfigException{
      val badRenamerConfigs = MongoSinkConfigSpec.basicGoodConfigs.clone().asInstanceOf[java.util.HashMap[String,String]]
      badRenamerConfigs.put("record.fields.rename","ts")
      badRenamerConfigs
    }

    validateConfigException{
      val badRenamerConfigs = MongoSinkConfigSpec.basicGoodConfigs.clone().asInstanceOf[java.util.HashMap[String,String]]
      badRenamerConfigs.put("record.fields.rename","ts=>")
      badRenamerConfigs
    }

    validateConfigException{
      val badRenamerConfigs = MongoSinkConfigSpec.basicGoodConfigs.clone().asInstanceOf[java.util.HashMap[String,String]]
      badRenamerConfigs.put("record.fields.rename","=>")
      badRenamerConfigs
    }

    validateConfigException{
      val badRenamerConfigs = MongoSinkConfigSpec.basicGoodConfigs.clone().asInstanceOf[java.util.HashMap[String,String]]
      badRenamerConfigs.put("record.fields.rename","=>=>")
      badRenamerConfigs
    }

    validateConfigException{
      val badRenamerConfigs = MongoSinkConfigSpec.basicGoodConfigs.clone().asInstanceOf[java.util.HashMap[String,String]]
      badRenamerConfigs.put("record.fields.rename","a=>b=>c")
      badRenamerConfigs
    }

    validateConfigException{
      val badRenamerConfigs = MongoSinkConfigSpec.basicGoodConfigs.clone().asInstanceOf[java.util.HashMap[String,String]]
      badRenamerConfigs.put("record.fields.rename","a,a=>b")
      badRenamerConfigs
    }

    validateConfigException{
      val badRenamerConfigs = MongoSinkConfigSpec.basicGoodConfigs.clone().asInstanceOf[java.util.HashMap[String,String]]
      badRenamerConfigs.put("record.fields.rename","a=>b,b=>c,d")
      badRenamerConfigs
    }
  }

  "good keys configs" should "parse correctly" in {
    val keysConfigs1 = MongoSinkConfigSpec.basicGoodConfigs.clone().asInstanceOf[java.util.HashMap[String,String]]
    keysConfigs1.put("record.keys","Id")

    val parsed = MongoSinkConfig.apply(keysConfigs1)
    parsed.recordKeys(0) should be("Id")

    val keysConfigs2 = MongoSinkConfigSpec.basicGoodConfigs.clone().asInstanceOf[java.util.HashMap[String,String]]
    keysConfigs2.put("record.keys","Id,Name")

    val parsed2 = MongoSinkConfig.apply(keysConfigs2)
    parsed2.recordKeys(0) should be("Id")
    parsed2.recordKeys(1) should be("Name")
  }

  "good fields configs" should "parse correctly" in {
    val fieldsConfigs1 = MongoSinkConfigSpec.basicGoodConfigs.clone().asInstanceOf[java.util.HashMap[String,String]]
    fieldsConfigs1.put("record.fields","Id")

    val parsed = MongoSinkConfig.apply(fieldsConfigs1)
    parsed.recordFields(0) should be("Id")

    val fieldsConfigs2 = MongoSinkConfigSpec.basicGoodConfigs.clone().asInstanceOf[java.util.HashMap[String,String]]
    fieldsConfigs2.put("record.fields","Id,Name")

    val parsed2 = MongoSinkConfig.apply(fieldsConfigs2)
    parsed2.recordFields(0) should be("Id")
    parsed2.recordFields(1) should be("Name")
  }

  def validateConfigException(f: => java.util.HashMap[String,String]): Unit ={
    var exceptionThrown = false

    try{
      MongoSinkConfig.apply(f)
    }
    catch{
      case _ : ConfigException => exceptionThrown = true
    }

    exceptionThrown should be(true)
  }
}

object MongoSinkConfigSpec {
  val mostBasicConfigs = new java.util.HashMap[String,String]()
  mostBasicConfigs.put("connector.class","com.startapp.data.MongoSinkConnector")
  mostBasicConfigs.put("tasks.max","5")
  mostBasicConfigs.put("db.host","localhost")
  mostBasicConfigs.put("db.port","27017")
  mostBasicConfigs.put("db.name","testdb")

  val basicConfigs = mostBasicConfigs.clone().asInstanceOf[java.util.HashMap[String,String]]
  basicConfigs.put("write.batch.enabled","true")
  basicConfigs.put("write.batch.size","200")
  basicConfigs.put("connect.use_schema","false")

  val basicGoodConfigs = basicConfigs.clone().asInstanceOf[java.util.HashMap[String,String]]
  basicGoodConfigs.put("db.collections","testCollection")
  basicGoodConfigs.put("topics","testTopic")
}
