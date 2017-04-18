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

import static org.apache.storm.kafka.spout.KafkaSpoutConfig.FirstPollOffsetStrategy.EARLIEST;

import java.util.List;
import java.util.regex.Pattern;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.storm.kafka.spout.Func;
import org.apache.storm.kafka.spout.KafkaSpoutConfig;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TridentKafkaClientWordCountWildcardTopics extends TridentKafkaClientWordCountNamedTopics {
    protected static final Logger LOG = LoggerFactory.getLogger(TridentKafkaClientWordCountWildcardTopics.class);
    
    private static final Pattern TOPIC_WILDCARD_PATTERN = Pattern.compile("test");//test-trident(-1)?    
    private static final String KAFKA_LOCAL_BROKER = "zythome:9092";
    private static final Func<ConsumerRecord<String, String>, List<Object>> JUST_VALUE_FUNC = new Func<ConsumerRecord<String, String>, List<Object>>() {
        @Override
        public List<Object> apply(ConsumerRecord<String, String> record) {
            LOG.info("ConsumerRecord:{}", record.value());
            return new Values(record.value());
        }
    };

    @Override
    protected KafkaSpoutConfig<String, String> newKafkaSpoutConfig() {
        return KafkaSpoutConfig.builder(KAFKA_LOCAL_BROKER, TOPIC_WILDCARD_PATTERN)
                .setGroupId("kafkaSpoutTestGroup")
                .setMaxPartitionFectchBytes(200)
                .setRecordTranslator(JUST_VALUE_FUNC, new Fields("str"))
                .setRetry(newRetryService())
                .setOffsetCommitPeriodMs(10_000)
                .setFirstPollOffsetStrategy(EARLIEST)
                .setMaxUncommittedOffsets(250)
                .build();
    }

    public static void main(String[] args) throws Exception {
        new TridentKafkaClientWordCountWildcardTopics().run(args);
    }
}
