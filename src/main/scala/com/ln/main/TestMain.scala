package com.ln.main

import com.ln.sink.RocketMqSink
import com.ln.source.RocketMqSource
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
object TestMain {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val stream = env.addSource(new RocketMqSource("nameServers","consumerGroup","topic",null))

    stream.addSink(new RocketMqSink("nameServers","producer","topic","tag"))

  }

}
