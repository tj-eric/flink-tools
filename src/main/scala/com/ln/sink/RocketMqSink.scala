package com.ln.sink

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.rocketmq.client.producer.DefaultMQProducer
import org.apache.rocketmq.common.message.Message
import org.apache.rocketmq.remoting.common.RemotingHelper
import org.slf4j.LoggerFactory

import scala.beans.BeanProperty


/**
  * 自定义实现flink sink
  * 以rocketmq为例
  */
class RocketMqSink extends RichSinkFunction[String] {

  val logger = LoggerFactory.getLogger(classOf[RocketMqSink])

  var producer: DefaultMQProducer = _
  @BeanProperty var nameServers: String = _
  @BeanProperty var productName: String = _
  @BeanProperty var topic: String = _
  @BeanProperty var tag: String = _
  private var isRunning = false


  def this(nameServers: String, productName: String, topic: String, tag: String) = {
    this()

    if (nameServers == null || productName == null || topic == null || tag == null) throw new Exception("param error : param have null")
    this.nameServers = nameServers
    this.productName = productName
    this.topic = topic
    this.tag = tag
    producer = new DefaultMQProducer()
    producer.setInstanceName(this.productName)
    producer.setNamesrvAddr(this.nameServers)

  }


  override def invoke(value: String, context: SinkFunction.Context[_]): Unit = {
    if (!isRunning) {
      logger.info("[SINK-MSG] start running")
      producer.start()
      producer.send(new Message(this.topic, this.tag, value.getBytes(RemotingHelper.DEFAULT_CHARSET)))
    }

  }

  override def close(): Unit = {
    super.close()
    isRunning = false
    if (producer != null) {
      producer.shutdown()
    }
    logger.info("[SINK-MSG] shutdown")
  }

}
