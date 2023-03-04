package com.github.aalopatin.utils

import com.typesafe.config.ConfigFactory

object ConfigUtils {
  def getField(): Seq[Seq[String]] = {
    val config = ConfigFactory.load()
    val field = config.getString("field")

    val regexp = "(\\([\\d*,*]*\\))".r
    val stacks = regexp.findAllIn(field).toSeq

    for (stack <- stacks) yield
      if (stack == "()") {
        Seq.empty[String]
      } else
        stack.replaceAll("[()]", "").split(",").toSeq
  }
}
