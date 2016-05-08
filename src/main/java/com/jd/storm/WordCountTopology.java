package com.jd.storm;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import com.jd.storm.bolt.PrintBolt;
import com.jd.storm.bolt.WordCountBolt;
import com.jd.storm.bolt.WordNormalizerBolt;
import com.jd.storm.spout.RandomSentenceSpout;

/**
 * Created by qiuxiangu on 2016/5/8.
 */
public class WordCountTopology {
    private static TopologyBuilder builder = new TopologyBuilder();

    public static void main(String[] args) {
        Config config = new Config();
        builder.setSpout("RandomSentence", new RandomSentenceSpout(),2);
        builder.setBolt("WordNormallizer", new WordNormalizerBolt(),2).shuffleGrouping("RandomSentence");
        builder.setBolt("WordCount", new WordCountBolt(),2).fieldsGrouping("WordNormallizer", new Fields("word"));
        builder.setBolt("Print", new PrintBolt(), 1).shuffleGrouping("WordCount");
        config.setDebug(false);
        if (args != null && args.length > 0) {
            try {
                config.setNumWorkers(1);
                StormSubmitter.submitTopology(args[0], config,
                        builder.createTopology());
            } catch (Exception e) {
                e.printStackTrace();
            }
        } else {
            config.setMaxTaskParallelism(1);
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("wordcount", config, builder.createTopology());
        }
    }
}
