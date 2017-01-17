package com.startapp.data

import java.util

import org.apache.kafka.connect.data.Struct
import org.apache.kafka.connect.data.Schema._
import sun.reflect.generics.reflectiveObjects.NotImplementedException

import scala.collection.JavaConversions._

/**
  * Created by Raz on 10/01/2017.
  */
trait ConnectMongoConverter {
  def toJsonMap(value: Object): List[(String, Object)]
}

object SchemaConnectMongoConverter extends ConnectMongoConverter {
  override def toJsonMap(value: Object): List[(String, Object)] = {
    val struct = value.asInstanceOf[Struct]
    var res : Map[String,Object] = Map()

    for (field <- struct.schema().fields()){
      val fieldName = field.name()
      val fieldType = field.schema().`type`()

      fieldType match {
        case Type.INT8 => res += (fieldName-> struct.getInt8(fieldName))
        case Type.INT16 => res += (fieldName-> struct.getInt16(fieldName))
        case Type.INT32 => res += (fieldName-> struct.getInt32(fieldName))
        case Type.INT64 => res += (fieldName-> struct.getInt64(fieldName))
        case Type.FLOAT32 => res += (fieldName-> struct.getFloat32(fieldName))
        case Type.FLOAT64 => res += (fieldName-> struct.getFloat64(fieldName))
        case Type.BOOLEAN => res += (fieldName-> struct.getBoolean(fieldName))
        case Type.STRING => res += (fieldName-> struct.getString(fieldName))
        case _ => throw new NotImplementedException()
      }
    }

    res.toList
  }
}

object NoSchemaConnectMongoConverter extends ConnectMongoConverter {
  override def toJsonMap(value: Object): List[(String, Object)] = {
    value.asInstanceOf[util.HashMap[String,Object]].toList
  }
}
