package ru.okabanov.challenge.utils

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper

object SimpleScalaObjectMapper extends ObjectMapper with ScalaObjectMapper {
  registerModule(DefaultScalaModule)
}
