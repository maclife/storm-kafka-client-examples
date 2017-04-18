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
import org.apache.storm.LocalDRPC;
import org.apache.storm.generated.StormTopology;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

/**
 * http://blog.csdn.net/wb81074/article/details/50378236
 *
 * @author zyt
 */
public class LocalSubmitter {

    protected static final Logger LOG = LoggerFactory.getLogger(LocalSubmitter.class);

    private LocalDRPC drpc;
    private LocalCluster cluster;

    public LocalSubmitter(LocalDRPC drpc, LocalCluster cluster) {
        this.drpc = drpc;
        this.cluster = cluster;
    }

    /**
     * LocalDRPC用来构建一个本地的DRPC Server。 LocalCluster用来构建本地的集群
     *
     * @return
     */
    public static LocalSubmitter newInstance() {
        return new LocalSubmitter(new LocalDRPC(), new LocalCluster());
    }

    public static Config defaultConfig() {
        return defaultConfig(false);
    }

    public static Config defaultConfig(boolean debug) {
        final Config conf = new Config();
        conf.setMaxSpoutPending(20);//设置spout task上面最多有多少个没有处理(ack/fail)的tuple，防止tuple队列过大,只对可靠任务起作用
        conf.setDebug(debug);
        //conf.setNumWorkers(2); // 设置工作进程,如果不添加端口,认会是4个worker进程
        return conf;
    }

    public LocalSubmitter(StormTopology topology, LocalDRPC drpc, LocalCluster cluster, String name) {
        this(drpc, cluster);
    }

    public void submit(String name, Config config, StormTopology topology) {
        cluster.submitTopology(name, config, topology);
    }


    public void printResults(int num, int time, TimeUnit unit) {
        for (int i = 0; i < num; i++) {
            try {              
                //--- DRPC RESULT: [["apple",74],["the",300],["and",123],["snow",60],["jumped",83]]
                LOG.info("--- DRPC RESULT: " + drpc.execute("words", "the and apple snow jumped"));
                Thread.sleep(unit.toMillis(time));
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    public void kill(String name) {
        cluster.killTopology(name);
    }

    public void shutdown() {
        cluster.shutdown();
    }

    public LocalDRPC getDrpc() {
        return drpc;
    }

    public LocalCluster getCluster() {
        return cluster;
    }
}
