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

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.kafka.spout.Func;
import org.apache.storm.kafka.spout.KafkaSpoutConfig;
import org.apache.storm.kafka.spout.KafkaSpoutRetryExponentialBackoff;
import org.apache.storm.kafka.spout.KafkaSpoutRetryExponentialBackoff.TimeInterval;
import org.apache.storm.kafka.spout.KafkaSpoutRetryService;
import org.apache.storm.kafka.spout.trident.KafkaTridentSpoutOpaque;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.storm.kafka.StringScheme;
import org.apache.storm.kafka.ZkHosts;

import static org.apache.storm.kafka.spout.KafkaSpoutConfig.FirstPollOffsetStrategy.EARLIEST;
import org.apache.storm.spout.SchemeAsMultiScheme;

public class TridentKafkaClientWordCountNamedTopics {

    private static final String TOPIC_1 = "test";
    private static final String TOPIC_2 = "test-trident-1";
    private static final String KAFKA_LOCAL_BROKER = "zytwork:9092";

    /**
     * Core KafkaSpout only accepts an instance of SpoutConfig. <br>
     * TridentKafkaConfig is another extension of KafkaConfig. <br>
     * TridentKafkaEmitter only accepts TridentKafkaConfig. <br>
     * @link https://github.com/apache/storm/tree/master/external/storm-kafka#core-spout
     */
    private OpaqueTridentKafkaSpout newOpaqueTridentKafkaSpout(TridentKafkaConfig conf) {
        return new OpaqueTridentKafkaSpout(conf);
    }

    private TransactionalTridentKafkaSpout newTransactionalTridentKafkaSpout(TridentKafkaConfig conf) {
        return new TransactionalTridentKafkaSpout(conf);
    }

    /**
     * 使用OpaqueTridentKafkaSpout时,默认的输出名称为bytes,其输出格式也并不是String字符串而是byte[], 需要根据编码格式将其转换为String
     *
     * @return
     */
    private KafkaTridentSpoutOpaque<String, String> newKafkaTridentSpoutOpaque() {
        return new KafkaTridentSpoutOpaque<>(newKafkaSpoutConfig());
    }

    protected static TridentKafkaConfig newTridentKafkaConfig(String zkUrl, String Topic) {
        ZkHosts hosts = new ZkHosts(zkUrl);
        TridentKafkaConfig config = new TridentKafkaConfig(hosts, Topic);
        config.scheme = new SchemeAsMultiScheme(new StringScheme());
        // Consume new data from the topic
        config.startOffsetTime = kafka.api.OffsetRequest.LatestTime();
        return config;
    }

    private static Func<ConsumerRecord<String, String>, List<Object>> JUST_VALUE_FUNC = new JustValueFunc();

    /**
     * Needs to be serializable
     */
    private static class JustValueFunc implements Func<ConsumerRecord<String, String>, List<Object>>, Serializable {

        @Override
        public List<Object> apply(ConsumerRecord<String, String> record) {
            return new Values(record.value());
        }
    };

    protected KafkaSpoutConfig<String, String> newKafkaSpoutConfig() {
        return KafkaSpoutConfig.builder(KAFKA_LOCAL_BROKER, TOPIC_1, TOPIC_2)
                .setGroupId("kafkaSpoutTestGroup_" + System.nanoTime())
                .setMaxPartitionFectchBytes(200)
                .setRecordTranslator(JUST_VALUE_FUNC, new Fields("str"))
                .setRetry(newRetryService())
                .setOffsetCommitPeriodMs(10_000)
                .setFirstPollOffsetStrategy(EARLIEST)
                .setMaxUncommittedOffsets(250)
                .build();
    }

    //KafkaSpoutRetryExponentialBackoff(TimeInterval initialDelay, TimeInterval delayPeriod, int maxRetries, TimeInterval maxDelay)
    protected KafkaSpoutRetryService newRetryService() {
        return new KafkaSpoutRetryExponentialBackoff(
                new TimeInterval(1000, TimeUnit.MICROSECONDS),
                TimeInterval.milliSeconds(2),
                Integer.MAX_VALUE,
                TimeInterval.seconds(10)
        );
    }

    public static void main(String[] args) throws Exception {
        new TridentKafkaClientWordCountNamedTopics().run(args);

    }

    protected void run(String[] args) throws AlreadyAliveException, InvalidTopologyException, AuthorizationException, InterruptedException {
        if (args.length > 0 && Arrays.binarySearch(args, "-h") >= 0) {
            System.out.printf("Usage: java %s [%s] [%s] [%s] [%s]\n", getClass().getName(),
                    "broker_host:broker_port", "topic1", "topic2", "topology_name");
        } else {
            final String brokerUrl = args.length > 0 ? args[0] : KAFKA_LOCAL_BROKER;
            final String topic1 = args.length > 1 ? args[1] : TOPIC_1;
            final String topic2 = args.length > 2 ? args[2] : TOPIC_2;

            System.out.printf("Running with broker_url: [%s], topics: [%s, %s]\n", brokerUrl, topic1, topic2);

            Config tpConf = LocalSubmitter.defaultConfig(true);

            if (args.length == 4) { //Submit Remote                
                // Producers
                StormSubmitter.submitTopology(topic1 + "-producer", tpConf, KafkaProducerTopology.newTopology(brokerUrl, topic1));
                //StormSubmitter.submitTopology(topic2 + "-producer", tpConf, KafkaProducerTopology.newTopology(brokerUrl, topic2));
                // Consumer
                StormSubmitter.submitTopology("topics-consumer", tpConf, TridentKafkaConsumerTopology.newTopology(newKafkaTridentSpoutOpaque()));

                // Print results to console, which also causes the print filter in the consumer topology to print the results in the worker log
                Thread.sleep(2000);
                DrpcResultsPrinter.remoteClient().printResults(60, 1, TimeUnit.SECONDS);

            } else { //Submit Local

                final LocalSubmitter localSubmitter = LocalSubmitter.newInstance();

                try {
                    // Producers
                    localSubmitter.submit(topic1 + "-producer", tpConf, KafkaProducerTopology.newTopology(brokerUrl, topic1));
                    //localSubmitter.submit(topic2 + "-producer", tpConf, KafkaProducerTopology.newTopology(brokerUrl, topic2));
                    // Consumer
                    try {

                        localSubmitter.submit("topics-consumer", tpConf,
                                TridentKafkaConsumerTopology.newTopology(localSubmitter.getDrpc(), newKafkaTridentSpoutOpaque())
                        );
                        Thread.sleep(2000);
                        // print
                        localSubmitter.printResults(100, 1, TimeUnit.SECONDS);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }

                } finally {
                    // kill
                    localSubmitter.kill(topic1 + "-producer");
                    //localSubmitter.kill(topic2Tp);
                    localSubmitter.kill("topics-consumer");
                    // 睡眠 1000*6毫秒后手动结停止本地集权
                    Thread.sleep(1000 * 6);
                    localSubmitter.shutdown();
                }
            }
        }
        System.exit(0);     // Kill all the non daemon threads
    }
}
