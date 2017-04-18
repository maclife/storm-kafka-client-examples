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
    private static final String KAFKA_LOCAL_BROKER = "zythome:9092";
    private static final String zkUrl = "zythome:2181";

    /**
     * 使用OpaqueTridentKafkaSpout时,默认的输出名称为bytes,其输出格式也并不是String字符串而是byte[], 需要根据编码格式将其转换为String
     *
     * @return
     */
    private KafkaTridentSpoutOpaque<String, String> newKafkaTridentSpoutOpaque() {//测试可用@170418
        return new KafkaTridentSpoutOpaque<>(newKafkaSpoutConfig());
    }

    /**
     * imp IOpaquePartitionedTransactionalSpout
     * <p>
     * IOpaquePartitionedTransactionalSpout为了解决IPartitionedTransactionalSpout卡住问题,它不保证每次重发一个批次的消息所包含的tuple完全一致.<br>
     * 也就是说某个tuple可能第一次在txid=2的批次中出现,后面有可能在txid=5的批次中出现.这种情况只出现在当某一批次消息消费失败需要重发且恰巧消息中间件故障时.<br>
     * 这时,IOpaquePartitionedTransactionalSpout不是等待消息中间件故障恢复,而是先读取可读的partition.例如txid=2的批次在消费过程中失败了,需要重发,恰巧消息中间件的16个分区有1个分区(partition=3)因为故障不可读了.<br>
     * 这时候IOpaquePartitionedTransactionalSpout会先读另外的15个分区,完成txid=2这个批次的发送,这时候同样的批次其实包含的tuple已经少了.<br>
     * 假设在txid=5时消息中间件的故障恢复了,那之前在txid=2且在分区partition=3的tuple会重新发送,包含在txid=5的批次中.<br>
     * 在使用IOpaquePartitionedTransactionalSpout时,因为tuple与txid的对应关系有可能改变,因此与业务计算结果同时保存一个txid就无法保证事务性了.<br>
     * 这时候解决方案会稍微复杂一些,除了保存业务计算结果以外,还要保存两个元素：前一批次的业务计算结果以及本批次的事务ID<br>
     * </p>
     *
     * @param conf
     * @return
     */
    private OpaqueTridentKafkaSpout newOpaqueTridentKafkaSpout(TridentKafkaConfig conf) {//测试可用@170418
        return new OpaqueTridentKafkaSpout(conf);
    }

    /**
     * <p>
     * IPartitionedTransactionalSpout和IOpaquePartitionedTransactionalSpout都是把tuple封装成batch进行处理,同时可以保证每一个tuple都被完整地处理,都支持消息重发.为了支持事务性,它们为每一个批次（batch）提供一个唯一的事务ID（transaction
     * id：txid）,txid是顺序递增的,而且保证对批次的处理是强有序的,即必须完整处理完txid=1才能再接着处理txid=2. IPartitionedTransactionalSpout提供了一种最简单的处理事务型应用的方法,持久化存储中要额外保存txid,它可能会出现spout卡住的问题.
     * IOpaquePartitionedTransactionalSpout是最严谨的处理事务型应用的方法,但是使用它编写应用代码会更复杂一些,持久化存储中除了要额外保存txid之外,还要保存前一批次的业务计算结果.
     * </p>
     */
    //---------------------------------------------------------------
    /**
     * imp IPartitionedTransactionalSpout
     * <p>
     * IPartitionedTransactionalSpout的每一个tuple都会绑定在固定的批次中.无论一个tuple重发多少次,它都在同一个批次里面,都有同样的事务ID;<br>
     * 一个tuple不会出现在两个以上的批次里.一个批次无论重发多少次,它也只有一个唯一且相同的事务ID,不会改变.这也就是说,一个批次无论重发多少次,它所包含的内容都是完全一致的.<br>
     * 但是IPartitionedTransactionalSpout会有一个问题,虽然这种问题非常罕见：<br>
     * 假设一批消息在被bolt消费过程中失败了,需要spout重发,此时如果正巧遇到消息发送中间件故障, 例如某一个分区不可读,spout为了保证重发时每一批次包含的tuple一致,它只能等待消息中间件恢复,也就是卡在那里无法再继续发送给bolt消息了,直至消息中间件恢复.<br>
     * IOpaquePartitionedTransactionalSpout可以解决这个问题.<br>
     * 通常情况下,IPartitionedTransactionalSpout已经可以满足大部分的事务型需求了<br>
     * </p>
     *
     * @param conf
     * @return
     */
    private TransactionalTridentKafkaSpout newTransactionalTridentKafkaSpout(TridentKafkaConfig conf) {
        return new TransactionalTridentKafkaSpout(conf);
    }

    /**
     * Core KafkaSpout only accepts an instance of SpoutConfig. <br>
     * <b>TridentKafkaConfig is another extension of KafkaConfig. </b>TridentKafkaEmitter only accepts TridentKafkaConfig. <br>
     * @link https://github.com/apache/storm/tree/master/external/storm-kafka#core-spout
     */
    protected static TridentKafkaConfig newTridentKafkaConfig(String zkUrl, String Topic) {//测试可用@170418
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
                        /*
                        localSubmitter.submit("topics-consumer", tpConf,
                                TridentKafkaConsumerTopology.newTopology(localSubmitter.getDrpc(), newKafkaTridentSpoutOpaque())
                        );
                         */
                        localSubmitter.submit("topics-consumer", tpConf,
                                TridentKafkaConsumerTopology.newTopology(localSubmitter.getDrpc(),
                                        newOpaqueTridentKafkaSpout(newTridentKafkaConfig(zkUrl, topic1))
                                )
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
