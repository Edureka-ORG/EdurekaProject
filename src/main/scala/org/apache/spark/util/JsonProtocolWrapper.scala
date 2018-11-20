package org.apache.spark.util;

import org.apache.spark.scheduler.SparkListenerEvent;
import org.json4s.JsonAST.JValue;

object JsonProtocolWrapper {
  def sparkEventToJson(event: SparkListenerEvent): JValue = {
    return JsonProtocol.sparkEventToJson(event);
  }

  def sparkEventFromJson(json: JValue): SparkListenerEvent = {
    return JsonProtocol.sparkEventFromJson(json);
  }
}
