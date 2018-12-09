package com.ln.source

import java.util
import java.util.UUID

import com.ln.constant.RocketmqConstant
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.source.{ParallelSourceFunction, RichParallelSourceFunction, RichSourceFunction, SourceFunction}
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer
import org.apache.rocketmq.client.consumer.listener.{ConsumeConcurrentlyContext, ConsumeConcurrentlyStatus, MessageListenerConcurrently}
import org.apache.rocketmq.common.consumer.ConsumeFromWhere
import org.apache.rocketmq.common.message.MessageExt
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel
import org.slf4j.LoggerFactory

import scala.beans.BeanProperty

/**
  * 自定义flinksource
  * 以实现rocketmq为例
  */
class RocketMqSource extends RichSourceFunction[String] with ParallelSourceFunction[String] {

  val logger = LoggerFactory.getLogger(classOf[RocketMqSource])

  @BeanProperty var consumer: DefaultMQPushConsumer = _
  @BeanProperty var isRunning = false
  @BeanProperty var isHasData = true

  @BeanProperty var charsetName: String = _
  @BeanProperty var nameServers: String = _
  @BeanProperty var consumerGroup: String = _
  @BeanProperty var topic: String = _
  @BeanProperty var consumerOffsetType: ConsumeFromWhere = _
  @BeanProperty var minThreadNum: Int = 0
  @BeanProperty var maxThreadNum: Int = 0
  @BeanProperty var maxBatchSize: Int = 0

  def this(nameServers: String, consumerGroup: String, topic: String, consumerOffsetType: ConsumeFromWhere) = {
    this()

    if (nameServers == null || consumerGroup == null || topic == null || minThreadNum < 0 || maxThreadNum < 0 || maxBatchSize < 0) throw new Exception("init RocketMQSource Fail , init information has null : nameServers = " + nameServers + ", consumerGroup = " + consumerGroup + ", topic = " + topic + ", minThreadNum = " + minThreadNum + ", maxThreadNum = " + maxThreadNum + ", maxBatchSize = " + maxBatchSize)

    this.nameServers = nameServers
    this.consumerGroup = consumerGroup
    this.topic = topic
    this.consumerOffsetType = consumerOffsetType
    this.minThreadNum = RocketmqConstant.ROCKETMQ_MIN_THREAD_NUM
    this.maxThreadNum = RocketmqConstant.ROCKETMQ_MAX_THREAD_NUM
    this.maxBatchSize = RocketmqConstant.ROCKETMQ_MAX_BATCH_SIZE

  }


  override def run(sourceContext: SourceFunction.SourceContext[String]): Unit = {
    logger.info("[SOURCE-ROCKETMQ] running method run " + isRunning)
    if (!isRunning) {
      consumer.registerMessageListener {
        new MessageListenerConcurrently() {
          override def consumeMessage(list: util.List[MessageExt], consumeConcurrentlyContext: ConsumeConcurrentlyContext): ConsumeConcurrentlyStatus = {
            //                logger.info("[SOURCE-MSG] comsume start");
            import scala.collection.JavaConversions._
            for (msg <- list) {
              isHasData = true
              try
                sourceContext.collect(new String(msg.getBody))
              //                        logger.info("[SOURCE-MSG-CHARSET] {}", charsetName);
              catch {
                case e: Exception =>
                  e.printStackTrace()
                  logger.info("[SOURCE-MSG] error : {}", e.toString)
                  // 重试
                  return ConsumeConcurrentlyStatus.RECONSUME_LATER
              }
            }
            isHasData = false
            ConsumeConcurrentlyStatus.CONSUME_SUCCESS
          }
        }
      }
      consumer.start()
      isRunning = true
    }
    logger.info("[SOURCE-ROCKETMQ] running method run {}", isRunning)
    //设置消费时间间隔1s
    while ( {
      isRunning
    }) Thread.sleep(1000)


  }


  override def open(parameters: Configuration): Unit = {
    logger.info("[SOURCE-ROCKETMQ] running method open ")
    super.open(parameters)
    if (consumer == null) {
      consumer = new DefaultMQPushConsumer
      charsetName = "UTF-8"
      consumer.setInstanceName(UUID.randomUUID.toString)
      consumer.setMessageModel(MessageModel.CLUSTERING) // 消费模式

      consumer.setConsumeFromWhere(if (consumerOffsetType == null) ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET
      else consumerOffsetType)
      consumer.setNamesrvAddr(nameServers)
      consumer.setConsumerGroup(consumerGroup)
      consumer.subscribe(topic, "*")
      consumer.setConsumeThreadMin(minThreadNum)
      consumer.setConsumeThreadMax(maxThreadNum)
      //消息数量每次读取的消息数量
      consumer.setConsumeMessageBatchMaxSize(maxBatchSize)
      logger.info("[SOURCE-ROCKETMQ] create consumer ")
    }
    logger.info("consumeBatchSize:{} pullBatchSize:{} consumeThread:{}", consumer.getConsumeMessageBatchMaxSize.toString, consumer.getPullBatchSize.toString, consumer.getConsumeThreadMax.toString)


  }

  override def cancel(): Unit = {
    super.close()
    logger.info("[SOURCE-ROCKETMQ] running method close ")
    if (consumer != null) consumer.shutdown()
    consumer = null
    isRunning = false

  }


  override def close(): Unit = {
    if (consumer != null) consumer.shutdown()
    consumer = null
    isRunning = false
  }
}
