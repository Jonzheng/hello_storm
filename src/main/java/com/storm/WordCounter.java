package com.storm;

import java.util.HashMap;
import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;

public class WordCounter implements IRichBolt{
	/**
	 * 
	 */
	private static final long serialVersionUID = 3661939002012629434L;
	Integer id;  
	String name;  
	Map<String, Integer> counters;  
	private OutputCollector collector;  

	public void cleanup() {
        System.out.println("-- Word Counter [" + name + "-" + id + "] --");  
        for (Map.Entry<String, Integer> entry : counters.entrySet()) {  
            System.out.println(entry.getKey() + ": " + entry.getValue());  
        }  
        counters.clear();  
	}

	public void execute(Tuple input) {
	   	 System.out.println("WC-execute--");
	     String str = input.getString(0);  
	     if (!counters.containsKey(str)) {  
	         counters.put(str, 1);  
	     } else {  
	         Integer c = counters.get(str) + 1;  
	         counters.put(str, c);  
	     }  
	     // 确认成功处理一个tuple  
	     collector.ack(input);  
	}

	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		// TODO Auto-generated method stub
        this.counters = new HashMap<String, Integer>();  
        this.collector = collector;  
        this.name = context.getThisComponentId();  
        this.id = context.getThisTaskId();  
	}

	public void declareOutputFields(OutputFieldsDeclarer arg0) {
		// TODO Auto-generated method stub
		
	}

	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

}
