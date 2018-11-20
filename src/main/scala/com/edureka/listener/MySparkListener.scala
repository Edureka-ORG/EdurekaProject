package com.edureka.listener

import org.apache.spark.SparkContext
import org.apache.spark.scheduler.SparkListener
import org.apache.spark.scheduler.SparkListenerApplicationEnd
import org.apache.spark.scheduler.SparkListenerApplicationStart
import org.apache.spark.scheduler.SparkListenerEvent
import org.apache.spark.scheduler.SparkListenerExecutorAdded
import org.apache.spark.scheduler.SparkListenerExecutorRemoved
import org.apache.spark.scheduler.SparkListenerJobEnd
import org.apache.spark.scheduler.SparkListenerJobStart
import org.apache.spark.scheduler.SparkListenerStageCompleted
import org.apache.spark.scheduler.SparkListenerStageSubmitted
import org.apache.spark.scheduler.SparkListenerTaskEnd
import org.apache.spark.scheduler.SparkListenerTaskGettingResult
import org.apache.spark.scheduler.SparkListenerTaskStart
import org.apache.spark.util.JsonProtocolWrapper

import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._
import com.edureka.producer.ProducerUtil
import org.apache.kafka.clients.producer.KafkaProducer

class MySparkListener extends SparkListener
{
  val topic = "BATCH29102018-TOPIC";
  var producer: KafkaProducer[String, String] = null;
  def publishEvent(event:SparkListenerEvent)
  {
    //Convert this event into understandable format... json
    //Key -- application-id
    //value -- json
    
    var appId = SparkContext.getOrCreate().applicationId
    var json = JsonProtocolWrapper.sparkEventToJson(event);
     val completeJson = ("timestamp" -> System.currentTimeMillis()) ~
      ("application-id" -> appId) ~
      ("spark-event" -> json)
      
     val publishJson = compact(render(completeJson));  

     producer= ProducerUtil.getProducer();
     
     ProducerUtil.send(producer, topic, appId,publishJson.toString());
      
       //This json will feed to kakfa
      
      
    
  }
    override def onStageCompleted(event: SparkListenerStageCompleted): Unit = { 
      //CaptureEvent
      publishEvent(event)
    }

  override def onStageSubmitted(event: SparkListenerStageSubmitted): Unit = { 
    publishEvent(event)
  }

  override def onTaskStart(event: SparkListenerTaskStart): Unit = { 
    publishEvent(event)
  }

  override def onTaskGettingResult(event: SparkListenerTaskGettingResult): Unit = { 
    publishEvent(event)
  }

  override def onTaskEnd(event: SparkListenerTaskEnd): Unit = {
    publishEvent(event)
  }

  override def onJobStart(event: SparkListenerJobStart): Unit = {
    publishEvent(event)
  }

  override def onJobEnd(event: SparkListenerJobEnd): Unit = { 
    publishEvent(event)
  }

//  override def onEnvironmentUpdate(environmentUpdate: SparkListenerEnvironmentUpdate): Unit = { }
//
//  override def onBlockManagerAdded(blockManagerAdded: SparkListenerBlockManagerAdded): Unit = { }
//
//  override def onBlockManagerRemoved(
//      blockManagerRemoved: SparkListenerBlockManagerRemoved): Unit = { }
//
//  override def onUnpersistRDD(unpersistRDD: SparkListenerUnpersistRDD): Unit = { }

  override def onApplicationStart(event: SparkListenerApplicationStart): Unit = { 
    publishEvent(event)
  }

  override def onApplicationEnd(event: SparkListenerApplicationEnd): Unit = { 
    publishEvent(event)
    ProducerUtil.close(producer);
    
  }

//  override def onExecutorMetricsUpdate(
//      executorMetricsUpdate: SparkListenerExecutorMetricsUpdate): Unit = { }

  override def onExecutorAdded(event: SparkListenerExecutorAdded): Unit = { 
    publishEvent(event)
  }

  override def onExecutorRemoved(event: SparkListenerExecutorRemoved): Unit = { 
    publishEvent(event)
  }

//  override def onBlockUpdated(blockUpdated: SparkListenerBlockUpdated): Unit = { }

//  override def onOtherEvent(event: SparkListenerEvent): Unit = { }
}