package com.ln.sink;

import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JavaRocketMqSink extends RichSinkFunction<String> {

    Logger logger = LoggerFactory.getLogger(JavaRocketMqSink.class);

    private DefaultMQProducer producer;
    private String nameServers;
    private String productName;
    private String topic;
    private String tag;
    private Boolean isRunning = false;

    public JavaRocketMqSink(String nameServers, String productName, String topic, String tag) throws Exception {
        if (nameServers == null || productName == null || topic == null || tag == null)
            throw new Exception("param error : param have null");
        this.nameServers = nameServers;
        this.productName = productName;
        this.topic = topic;
        this.tag = tag;
        producer = new DefaultMQProducer();
        producer.setInstanceName(this.productName);
        producer.setNamesrvAddr(this.nameServers);
    }

    @Override
    public void invoke(String value, Context context) throws Exception {
        if (!isRunning) {
            logger.info("[SINK-MSG] start running");
            producer.start();
            producer.send(new Message(this.topic, this.tag, value.getBytes(RemotingHelper.DEFAULT_CHARSET)));
        }
    }

    @Override
    public void close() throws Exception {
        super.close();
        isRunning = false;
        if (producer != null) {
            producer.shutdown();
        }
        logger.info("[SINK-MSG] shutdown");
    }


}
