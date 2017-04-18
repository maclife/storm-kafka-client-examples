/*
 * Licensed to the Apache Software Foundation (ASF) under one
 *   or more contributor license agreements.  See the NOTICE file
 *   distributed with this work for additional information
 *   regarding copyright ownership.  The ASF licenses this file
 *   to you under the Apache License, Version 2.0 (the
 *   "License"); you may not use this file except in compliance
 *   with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */
package org.apache.storm.kafka.trident;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.kafka.bolt.KafkaBolt;
import org.apache.storm.kafka.bolt.mapper.FieldNameBasedTupleToKafkaMapper;
import org.apache.storm.kafka.bolt.selector.DefaultTopicSelector;
import org.apache.storm.starter.spout.RandomSentenceSpout;
import org.apache.storm.topology.TopologyBuilder;

import java.util.Properties;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
/**
 * KafkaProducerTopology 可独立运行 <br>
 * 首先启动zookeeper 和kafka server
 *
 * @author zyt
 */
public class KafkaProducerTopology {

    protected static final Logger LOG = LoggerFactory.getLogger(KafkaProducerTopology.class);

    /**
     * @param brokerUrl Kafka broker URL
     * @param topicName Topic to which publish sentences
     * @return A Storm topology that produces random sentences using {@link RandomSentenceSpout} and uses a {@link KafkaBolt} to publish the sentences to the
     * kafka topic specified
     */
    public static StormTopology newTopology(String brokerUrl, String topicName) {
        LOG.info("KafkaProducerTopology.newTopology:" + topicName);
        final TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("spout", new RandomSentenceSpout.TimeStamped(""), 2);
        /* The output field of the RandomSentenceSpout ("word") is provided as the boltMessageField
          so that this gets written out as the message in the kafka topic. */
        LOG.info("Create a KafkaBolt");
        //给KafkaBolt配置topic及前置tuple消息到kafka的mapping关系  
        final KafkaBolt<String, String> bolt = new KafkaBolt<String, String>()
                .withProducerProperties(newProps(brokerUrl, topicName))
                .withTopicSelector(new DefaultTopicSelector(topicName))                
                .withTupleToKafkaMapper(new FieldNameBasedTupleToKafkaMapper("key", "word"));
        //<String, String>withTupleToKafkaMapper(new FieldNameBasedTupleToKafkaMapper<String, String>("key", "word"));
        LOG.info("KafkaBolt forward To afka");

        builder.setBolt("forwardToKafka", bolt, 1).shuffleGrouping("spout");

        return builder.createTopology();
    }

    /**
     * @return the Storm config for the topology that publishes sentences to kafka using a kafka bolt.
     */
    private static Properties newProps(final String brokerUrl, final String topicName) {
        return new Properties() {
            { 
                put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerUrl);
                put("acks", "1");
                put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
                put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
                put(ProducerConfig.CLIENT_ID_CONFIG, topicName);

            }
        };
    }

    //必须使用机器名zythome,使用localhost或IP都不行
    //storm 不必开启    
    public static void main(String[] args) throws Exception {
        String brokerUrl = "zythome:9092";

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("wordCounter",  new Config(), newTopology(brokerUrl, "test"));        
        Thread.sleep(60 * 1000 * 10);
        cluster.killTopology("wordCounter");
        cluster.shutdown();

    }
}
