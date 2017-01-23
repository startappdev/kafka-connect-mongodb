package com.startapp.data.validators

import java.util

import org.apache.kafka.common.config.ConfigDef.Validator
import org.apache.kafka.common.config.ConfigException
import org.scalatest.{FlatSpec, Matchers}

/**
  * Created by Raz on 22/01/2017.
  */
class RenameListValidatorSpec extends FlatSpec with Matchers {

  "ensure valid of good rename list configurations" should "not throw an exception" in {
    RenameListValidator.ensureValid("", null)

    val l1 = new util.ArrayList[String]()
    l1.add("ts=>timestamp")
    l1.add("num=>number")
    RenameListValidator.ensureValid("", l1)
  }

  "ensure valid of bad rename list configurations" should "throw exception for each validation" in {
    validateConfigException{
      val l = new util.ArrayList[String]()
      l.add("ts")
      l
    }

    validateConfigException{
      val l = new util.ArrayList[String]()
      l.add("ts=>")
      l
    }

    validateConfigException{
      val l = new util.ArrayList[String]()
      l.add("ts=>=>")
      l
    }

    validateConfigException{
      val l = new util.ArrayList[String]()
      l.add("=>")
      l
    }

    validateConfigException{
      val l = new util.ArrayList[String]()
      l.add("=>aa=>xx")
      l
    }
  }

  def validateConfigException(f: => java.util.List[String]): Unit ={
    var exceptionThrown = false

    try{
      RenameListValidator.ensureValid("", f)
    }
    catch{
      case _ : ConfigException => exceptionThrown = true
    }

    exceptionThrown should be(true)
  }
}
