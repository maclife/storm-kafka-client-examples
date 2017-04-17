/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.apache.storm.kafka.trident;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Arrays;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.kafka.BrokerHosts;
import org.apache.storm.kafka.StringScheme;
import org.apache.storm.kafka.ZkHosts;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.operation.BaseFunction;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.operation.builtin.Count;
import org.apache.storm.trident.testing.MemoryMapState;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SyTopology {

    public static final Logger LOG = LoggerFactory.getLogger(SyTopology.class);

    private final BrokerHosts brokerHosts;

    public SyTopology(String kafkaZookeeper) {
        brokerHosts = new ZkHosts(kafkaZookeeper);
    }

    public StormTopology buildTopology() {
        //定义kafka想着配置
        TridentKafkaConfig kafkaConfig = new TridentKafkaConfig(brokerHosts, "test");
        kafkaConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
        // TransactionalTridentKafkaSpout kafkaSpout = new TransactionalTridentKafkaSpout(kafkaConfig);  
        OpaqueTridentKafkaSpout kafkaSpout = new OpaqueTridentKafkaSpout(kafkaConfig);
        
        TridentTopology topology = new TridentTopology();
        //从kafka中读取消息并处理
        topology.newStream("kafka", kafkaSpout)
                .each(new Fields("str"), new Split(), new Fields("word")).groupBy(new Fields("word"))
                .persistentAggregate(new MemoryMapState.Factory(), new Count(), new Fields("count"))
                .parallelismHint(16);

        return topology.build();
    }

    public static void main(String[] args) throws AlreadyAliveException, InvalidTopologyException, AuthorizationException, InterruptedException {
        SyTopology topology = new SyTopology("zythome:9092");
        Config config = new Config();
        config.put(Config.TOPOLOGY_TRIDENT_BATCH_EMIT_INTERVAL_MILLIS, 2000);

        String dockerIp = "zythome";
        config.setNumWorkers(9);
        config.setMaxTaskParallelism(5);
        config.put(Config.STORM_ZOOKEEPER_PORT, 2181);
        config.put(Config.STORM_ZOOKEEPER_SERVERS, Arrays.asList(dockerIp));
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("wordCounter", config, topology.buildTopology());
        Thread.sleep(60 * 1000 * 10);
        cluster.killTopology("wordCounter");

        cluster.shutdown();

    }

    static class Split extends BaseFunction {

        @Override
        public void execute(TridentTuple tuple, TridentCollector collector) {
            String sentence = tuple.getString(0);
            for (String word : sentence.split(",")) {
                try {
                    FileWriter fw = new FileWriter(new File("test.txt"), true);
                    fw.write(word);
                    fw.flush();
                    fw.close();
                } catch (IOException e) {
                    // TODO Auto-generated catch block  
                    e.printStackTrace();
                }
                collector.emit(new Values(word));

            }
        }
    }
}
