package kafa

/**
 * Created by lenovo on 2017/12/28.
 */
/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// scalastyle:off println
package spark.streaming.chinaunicom_pro2

import kafka.serializer.StringDecoder
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka.KafkaUtils

/**


 * Example:
 *    $ bin/run-example streaming.DirectKafkaWordCount broker1-host:port,broker2-host:port \
 *    topic1,topic2

/home/spark2/bin/spark-submit --class spark.streaming.chinaunicom_pro2.Kafka_test1 \
 --jars /home/yimr/sss/spark-streaming-kafka-0-8_2.11-2.1.0.jar,/home/yimr/sss/kafka_2.10-0.8.0.jar,/home/yimr/sss/kafka-clients-0.8.2.1.jar \
/home/yimr/sss/lqyyhv_test.jar 10.162.2.83:9092,10.162.2.84:9092,10.162.2.85:9092,10.162.2.86:9092,10.162.2.87:9092 TP_C017_E01
**/
class ReceiveKafka {

  private val master = "172.16.2.31"
  private val port = "7077"

  private val data_output = "hdfs://172.16.2.31:8020/user/yimr/sss/kafka_tmp1"

  def main(args: Array[String]) {
    if (args.length < 2) {
      System.err.println(s"""
        |Usage: DirectKafkaWordCount <brokers> <topics>
        |  <brokers> is a list of one or more Kafka brokers
        |  <topics> is a list of one or more kafka topics to consume from
        |
        """.stripMargin)
      System.exit(1)
    }

    val Array(brokers, topics) = args

    val sparkConf = new SparkConf()
      .set("spark.executor.memory","3g")
      .set("spark.streaming.kafka.maxRatePerPartition", "2000")
      .setMaster(s"spark://$master:$port")
      .setAppName("DirectKafka")
    val ssc = new StreamingContext(sparkConf, Seconds(3))

    // Create direct kafka stream with brokers and topics
    val topicsSet = topics.split(",").toSet

    val kafkaParams = Map[String, String](
      "bootstrap.servers" -> brokers.toString,
      "group.id" -> "new_4_4",
      "auto.offset.reset" -> "smallest", //smallest   largest
      "enable.auto.commit" -> "false"
    )

    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicsSet)
    ssc.start()
    ssc.awaitTermination()
  }

}
