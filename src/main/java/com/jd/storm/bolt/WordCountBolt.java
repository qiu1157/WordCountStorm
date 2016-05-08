package com.jd.storm.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.jd.storm.util.MapSort;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by qiuxiangu on 2016/5/8.
 */
public class WordCountBolt implements IRichBolt {
    Map<String,Integer> counters;
    private OutputCollector outputCollector;
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        outputCollector = outputCollector;
        counters = new HashMap<String, Integer>();
    }

    public void execute(Tuple tuple) {
        String str = tuple.getString(0);
        if(!counters.containsKey(str)) {
            counters.put(str,1);
        }else {
            Integer c = counters.get(str) + 1;
            counters.put(str,c);
        }

        int num = 8;
        int length = 0;
        counters = MapSort.sortByValue(counters);
        if (num < counters.keySet().size()) {
            length = num;
        }else {
            length = counters.keySet().size();
        }

        String word = null;

        int count = 0;
        for(String key : counters.keySet()) {
            if(count >= length) {
                break;
            }
            if(count == 0){
                word = "[" + key + ":" + counters.get(key) + "]";
            }else {
                word = word + "[" + key + ":" + counters.get(key) + "]";
            }
            count++;
        }

        word = "The first " + num + ": "+ word;
        outputCollector.emit(new Values(word));
    }

    public void cleanup() {

    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("word"));
    }

    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
