package com.startapp.data

import java.lang.Boolean
import java.util

import org.apache.kafka.connect.data.{Schema, SchemaBuilder, Struct}
import org.scalatest.{FlatSpec, Matchers}
/**
  * Created by Raz on 22/01/2017.
  */
class ConnectMongoConverterSpec extends FlatSpec with Matchers{
  private val FIELD1_NAME = "fieldInt"
  private val FIELD1_VALUE = new Integer(5)
  private val FIELD2_NAME = "fieldString"
  private val FIELD2_VALUE = "str"
  private val FIELD3_NAME = "fieldBoolean"
  private val FIELD3_VALUE = new Boolean(true)

  val schema = SchemaBuilder.struct().name("test schema")
    .field(FIELD1_NAME, Schema.INT32_SCHEMA)
    .field(FIELD2_NAME, Schema.STRING_SCHEMA)
    .field(FIELD3_NAME, Schema.BOOLEAN_SCHEMA)
    .build()

  "No Schema Connect Mongo Converter Bad Data" should "throw an exception" in {
    var exceptionThrown = false

    val badData = new Struct(schema)

    try{
      checkJsonMap(NoSchemaConnectMongoConverter, badData)
    }
    catch {
      case _ : java.lang.ClassCastException => exceptionThrown = true
    }

    exceptionThrown should be(true)
  }

  "No Schema Connect Mongo Converter Good Data" should "return the same map" in {
    val jsonMap = new util.HashMap[String, Object]()
    jsonMap.put(FIELD1_NAME, FIELD1_VALUE)
    jsonMap.put(FIELD2_NAME, FIELD2_VALUE)
    jsonMap.put(FIELD3_NAME, FIELD3_VALUE)

    checkJsonMap(NoSchemaConnectMongoConverter, jsonMap)
  }

  "Schema Connect Mongo Converter Bad Data" should "throw an exception" in {
    var exceptionThrown = false

    val badData = new util.HashMap[String, Object]()
    badData.put(FIELD1_NAME, FIELD1_VALUE)

    try {
      checkJsonMap(SchemaConnectMongoConverter, badData)
    }
    catch {
      case _ : java.lang.ClassCastException => exceptionThrown = true
    }

    exceptionThrown should be(true)
  }

  "Schema Connect Mongo Converter Good Data" should "convert data to json map" in {
    val data = new Struct(schema)
      .put(FIELD1_NAME, FIELD1_VALUE)
      .put(FIELD2_NAME, FIELD2_VALUE)
      .put(FIELD3_NAME, FIELD3_VALUE)

    checkJsonMap(SchemaConnectMongoConverter, data)
  }

  private def checkJsonMap(converter : ConnectMongoConverter, value: Object): Unit ={
    val newJsonMap = converter.toJsonMap(value).toMap

    newJsonMap(FIELD1_NAME) should be(FIELD1_VALUE)
    newJsonMap(FIELD2_NAME) should be(FIELD2_VALUE)
    newJsonMap(FIELD3_NAME) should be(FIELD3_VALUE)
  }

}
