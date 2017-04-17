/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.apache.storm.kafka.trident;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.LocalDRPC;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.trident.TridentState;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.operation.BaseFunction;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.operation.builtin.Count;
import org.apache.storm.trident.operation.builtin.FilterNull;
import org.apache.storm.trident.operation.builtin.MapGet;
import org.apache.storm.trident.operation.builtin.Sum;
import org.apache.storm.trident.testing.FixedBatchSpout;
import org.apache.storm.trident.testing.MemoryMapState;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

/**
 * TridentWordCount 可独立运行 <br>
 * 在内建的集群中运行,将结果保存于内存,不依赖外部环境
 *
 * @author zyt
 */
public class TridentWordCount {

    public static class Split extends BaseFunction {

        @Override
        public void execute(TridentTuple tuple, TridentCollector collector) {
            String sentence = tuple.getString(0);
            for (String word : sentence.split(" ")) {
                collector.emit(new Values(word));
            }
        }
    }

    public static StormTopology buildTopology(LocalDRPC drpc) {
        //这个是一个batch Spout  一次发3个..
        FixedBatchSpout spout = new FixedBatchSpout(
                new Fields("sentence"), 3,
                new Values("the cow jumped over the moon"),
                new Values("the man went to the store and bought some candy"),
                new Values("four score and seven years ago"),
                new Values("how many apples can you eat"),
                new Values("to be or not to be the person")
        );

        spout.setCycle(true);//Spout是否循环发送

        TridentTopology topology = new TridentTopology();
        TridentState wordCounts = topology.newStream("spout1", spout)
                .parallelismHint(2)
                .each(new Fields("sentence"), new Split(), new Fields("word"))//setbolt
                .groupBy(new Fields("word"))
                .persistentAggregate(new MemoryMapState.Factory(), new Count(), new Fields("count"))
                .parallelismHint(2);

        topology.newDRPCStream("words", drpc)
                .each(new Fields("args"), new Split(), new Fields("word"))
                .groupBy(new Fields("word"))
                .stateQuery(wordCounts, new Fields("word"), new MapGet(), new Fields("count"))
                .each(new Fields("count"), new FilterNull())
                .aggregate(new Fields("count"), new Sum(), new Fields("sum"));
        return topology.build();
    }

    public static void main(String[] args) throws Exception {
        Config conf = new Config();
        conf.setMaxSpoutPending(20);
        if (args.length == 0) {
            LocalDRPC drpc = new LocalDRPC();
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("wordCounter", conf, buildTopology(drpc));
            for (int i = 0; i < 100; i++) {
                ////持续输出对jumped和the的查询结果 
                System.out.println("DRPC RESULT: " + drpc.execute("words", "cat the dog jumped"));
                Thread.sleep(1000);
            }
        } else {
            conf.setNumWorkers(3);
            StormSubmitter.submitTopologyWithProgressBar(args[0], conf, buildTopology(null));
        }
    }
}
