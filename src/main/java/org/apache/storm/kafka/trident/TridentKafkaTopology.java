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

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.kafka.trident.mapper.FieldNameBasedTupleToKafkaMapper;
import org.apache.storm.kafka.trident.selector.DefaultTopicSelector;
import org.apache.storm.trident.Stream;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.testing.FixedBatchSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.util.Properties;

public class TridentKafkaTopology {

    private static StormTopology buildTopology(String brokerConnectionString) {
        Fields fields = new Fields("word", "count");
        FixedBatchSpout spout = new FixedBatchSpout(fields, 4,
                new Values("storm", "1"),
                new Values("trident", "1"),
                new Values("needs", "1"),
                new Values("javadoc", "1")
        );
        spout.setCycle(true);

        TridentTopology topology = new TridentTopology();
        Stream stream = topology.newStream("spout1", spout);

        Properties props = new Properties();
        props.put("bootstrap.servers", brokerConnectionString);
        props.put("acks", "1");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        TridentKafkaStateFactory stateFactory = new TridentKafkaStateFactory()
                .withProducerProperties(props)
                .withKafkaTopicSelector(new DefaultTopicSelector("test"))
                .withTridentTupleToKafkaMapper(new FieldNameBasedTupleToKafkaMapper("word", "count"));

        //partitionPersist 操作会更新一个State.其内部是将 State和一批更新的tuple交给StateUpdater,由StateUpdater完成相应的更新操作.
        //partitionPersist 会返回一个TridentState对象来表示被这个Trident topoloy更新过.然后你就可以使用这个state在topology的任何地方进行查询操作了.   
        stream.partitionPersist(stateFactory, fields, new TridentKafkaUpdater(), new Fields());

        return topology.build();
    }

    /**
     * To run this topology ensure you have a kafka broker running and provide connection string to broker as argument. Create a topic test with command line,
     * kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partition 1 --topic test
     *
     * run this program and run the kafka consumer: kafka-console-consumer.sh --zookeeper localhost:2181 --topic test --from-beginning
     *
     * you should see the messages flowing through.
     *
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        System.out.println("Please provide kafka broker url ,e.g. localhost:9092");

        String brokerUrl = args.length >= 1 ? args[0] : "zythome:9092";

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("wordCounter", new Config(), buildTopology(brokerUrl));
        Thread.sleep(60 * 1000);
        cluster.killTopology("wordCounter");

        cluster.shutdown();
    }
}
