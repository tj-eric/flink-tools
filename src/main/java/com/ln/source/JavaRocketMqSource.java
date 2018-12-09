package com.ln.source;

import com.ln.constant.RocketMqConstant;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

/**
 * java版自定义实现source
 * 以rocketmq为例
 */
public class JavaRocketMqSource extends RichSourceFunction<MessageExt>
        implements ParallelSourceFunction<MessageExt> {

    private static Logger logger = LoggerFactory.getLogger(JavaRocketMqSource.class);

    private DefaultMQPushConsumer consumer;
    private boolean isRunning = false;
    private boolean isHasData = true;

    private String charsetName;
    private String nameServers;
    private String consumerGroup;
    private String topic;
    private ConsumeFromWhere consumerOffsetType;
    private int minThreadNum;
    private int maxThreadNum;
    private int maxBatchSize;

    public JavaRocketMqSource(String nameServers, String consumerGroup, String topic, ConsumeFromWhere consumerOffsetType) throws Exception {

        if (nameServers == null || consumerGroup == null || topic == null) {
            throw new Exception("init RocketMQSource Fail , init information has null : nameServers = " + nameServers
                    + ", consumerGroup = " + consumerGroup
                    + ", topic = " + topic);
        }

        this.nameServers = nameServers;
        this.consumerGroup = consumerGroup;
        this.topic = topic;
        this.consumerOffsetType = consumerOffsetType;
        this.minThreadNum = RocketMqConstant.ROCKETMQ_MIN_THREAD_NUM;
        this.maxThreadNum = RocketMqConstant.ROCKETMQ_MAX_THREAD_NUM;
        this.maxBatchSize = RocketMqConstant.ROCKETMQ_MAX_BATCH_SIZE;
    }




    public void run(SourceContext<MessageExt> sourceContext) throws Exception {

        logger.info("[SOURCE-ROCKETMQ] running method run " + isRunning);
        if (!isRunning) {
            consumer.registerMessageListener((MessageListenerConcurrently) (list, consumeConcurrentlyContext) -> {

                logger.info("[SOURCE-MSG] comsume start");
                for (MessageExt msg : list) {
                    isHasData = true;
                    try {
                        sourceContext.collect(msg);
                    } catch (Exception e) {
                        e.printStackTrace();
                        logger.info("[SOURCE-MSG] error : {}", e.toString());
                        // 重试
                        return ConsumeConcurrentlyStatus.RECONSUME_LATER;
                    }
                }
                isHasData = false;
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            });
            consumer.start();
            isRunning = true;
        }
        logger.info("[SOURCE-ROCKETMQ] running method run {}", isRunning);
        while (isRunning) {
            Thread.sleep(1000);
        }

    }

    @Override
    public void open(Configuration parameters) throws Exception {

        logger.info("[SOURCE-ROCKETMQ] running method open ");
        super.open(parameters);
        if (consumer == null) {
            consumer = new DefaultMQPushConsumer();
            charsetName = "UTF-8";

            consumer.setInstanceName(UUID.randomUUID().toString());
            consumer.setMessageModel(MessageModel.CLUSTERING);// 消费模式
            consumer.setConsumeFromWhere(consumerOffsetType == null ? ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET : consumerOffsetType);

            consumer.setNamesrvAddr(nameServers);
            consumer.setConsumerGroup(consumerGroup);
            consumer.subscribe(topic, "*");


            consumer.setConsumeThreadMin(minThreadNum);
            consumer.setConsumeThreadMax(maxThreadNum);
            //消息数量每次读取的消息数量
            consumer.setConsumeMessageBatchMaxSize(maxBatchSize);
            logger.info("[SOURCE-ROCKETMQ] create consumer ");
        }
        logger.info("consumeBatchSize:{} pullBatchSize:{} consumeThread:{}", consumer.getConsumeMessageBatchMaxSize(), consumer.getPullBatchSize(), consumer.getConsumeThreadMax());
    }

    public void cancel() {
        if (consumer != null) {
            consumer.shutdown();
        }
        consumer = null;
        isRunning = false;
    }



    @Override
    public void close() throws Exception {
        super.close();
        logger.info("[SOURCE-ROCKETMQ] running method close ");
        if (consumer != null) {
            consumer.shutdown();
        }
        consumer = null;
        isRunning = false;
    }
}
