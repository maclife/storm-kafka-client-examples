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

import org.apache.storm.LocalDRPC;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.starter.trident.DebugMemoryMapState;
import org.apache.storm.trident.Stream;
import org.apache.storm.trident.TridentState;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.operation.BaseFilter;
import org.apache.storm.trident.operation.builtin.Count;
import org.apache.storm.trident.operation.builtin.Debug;
import org.apache.storm.trident.operation.builtin.FilterNull;
import org.apache.storm.trident.operation.builtin.MapGet;
import org.apache.storm.trident.spout.ITridentDataSource;
import org.apache.storm.trident.testing.MemoryMapState;

import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Fields;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 参考 <br>
 * http://www.flyne.org/article/216 <br>
 * http://brandnewuser.iteye.com/blog/2309970 <br>
 * http://blog.csdn.net/derekjiang/article/details/9126185<br>
 * http://www.cnblogs.com/hseagle/p/3516458.html<br>
 * 
 * StormTopology TridentTopology的区别
 * See http://www.tianshouzhi.com/api/tutorials/storm/56
 *
 * @author zyt
 */
public class TridentKafkaConsumerTopology {

    protected static final Logger LOG = LoggerFactory.getLogger(TridentKafkaConsumerTopology.class);

    /**
     * See {@link TridentKafkaConsumerTopology#newTopology(LocalDRPC, ITridentDataSource)}
     *
     * @param tridentSpout
     * @return
     */
    public static StormTopology newTopology(ITridentDataSource tridentSpout) {
        return newTopology(null, tridentSpout);
    }

    /**
     * @param drpc The DRPC stream to be used in querying the word counts. Can be null in distributed mode
     * @param tridentSpout
     * @return a trident topology that consumes sentences from the kafka topic specified using a {@link TransactionalTridentKafkaSpout} computes the word count
     * and stores it in a {@link MemoryMapState}.
     */
    public static StormTopology newTopology(LocalDRPC drpc, ITridentDataSource tridentSpout) {

        LOG.info("TridentKafkaConsumerTopology.newTopology");
        final TridentTopology tridentTopology = new TridentTopology();
        // The DRPC stream to be used in querying the word counts. Can be null in distributed mode
        addDRPCStream(tridentTopology, addTridentState(tridentTopology, tridentSpout), drpc);
        return tridentTopology.build();
    }

    private static Stream addDRPCStream(TridentTopology tridentTopology, final TridentState state, LocalDRPC drpc) {
        LOG.info("TridentKafkaConsumerTopology.addDRPCStream");
        return tridentTopology.newDRPCStream("words", drpc)//if(server==null)spout = new DRPCSpout(function);
                .each(new Fields("args"), new Split(), new Fields("word"))
                .groupBy(new Fields("word"))
                .stateQuery(state, new Fields("word"), new MapGet(), new Fields("count"))
                .each(new Fields("count"), new FilterNull())
                .project(new Fields("word", "count"))
                .filter(new BaseFilter() {
                    @Override
                    public boolean isKeep(TridentTuple tuple) {
                        LOG.info("IsKeep DRPC RESULT: {}",tuple);  // Used to show the DRPC results in the worker log. Useful for debugging
                        return true;
                    }
                });
        /**
         * 经Stream中的project方法处理后的tuple仅保持指定字段（相当于过滤字段）.<br>
         * 例如,mystream中的字段为 ["a", "b", "c", "d"],执行下面代码： mystream.project(new Fields("b", "d")) 则输出流将仅包含["b", "d"]字段.
         */
    }

    private static TridentState addTridentState(TridentTopology tridentTopology, ITridentDataSource tridentSpout) {
        LOG.info("TridentKafkaConsumerTopology.addTridentState");
        final Stream spoutStream = tridentTopology.newStream("spout1", tridentSpout).parallelismHint(2);
       
        return spoutStream.each(new Fields("str"), new Split(), new Fields("word"))
                .groupBy(new Fields("word"))
                .persistentAggregate(new MemoryMapState.Factory(), new Count(), new Fields("count"))
                .parallelismHint(2);

        //persistentAggregate是在partitionPersist之上的另外一层抽象.它知道怎么去使用一个Trident 聚合器来更新State
        //这个例子当中,一个group好的stream,Trident会期待你提供的state是实现了MapState接口(DebugMemoryMapState.Factory())
        //用来进行group的字段会以key的形式存在于State当中,聚合后的结果会以value的形式存储在State当中
    }
    
    
    
}
