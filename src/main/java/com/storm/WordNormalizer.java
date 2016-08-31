package com.storm;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

public class WordNormalizer extends BaseRichBolt{
	private static final long serialVersionUID = 4721613608468891421L;
	private OutputCollector collector;  

	public void execute(Tuple input) {
	     String sentence = input.getString(0);  
	     String[] words = sentence.split("\\s");  
	     for (String word : words) {  
	         word = word.trim();  
	         if (!word.isEmpty()) {
	             word = word.toLowerCase();  
	             // Emit the word  
	             List a = new ArrayList();  
	             a.add(input);  
	             collector.emit(a, new Values(word));  
	         }  
	     }  
	     //确认成功处理一个tuple  
	     collector.ack(input);  
	}

	public void prepare(Map arg0, TopologyContext arg1, OutputCollector collector) {
		this.collector = collector;  
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		 declarer.declare(new Fields("word"));
	}
}
