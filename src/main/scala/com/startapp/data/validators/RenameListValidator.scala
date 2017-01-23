package com.startapp.data.validators

import org.apache.kafka.common.config.ConfigDef.Validator
import org.apache.kafka.common.config.ConfigException

import scala.collection.JavaConversions._

/**
  * Validates connector's renamer config.
  * each item in the list must be formatted in this pattern: "originalValue=>newValue"
  */
object RenameListValidator extends Validator{
  override def ensureValid(name: String, o: scala.Any): Unit = {
    if(o != null){
      val l = o.asInstanceOf[java.util.List[String]].toList
      l.foreach{item=>
        if(item.split("=>").length != 2) {
          throw new ConfigException(name, o, "Each element must formatted in this pattern: x => y ")
        }
      }
    }
  }
}

//object RenameListValidator {
//  def apply: RenameListValidator = new RenameListValidator()
//}
