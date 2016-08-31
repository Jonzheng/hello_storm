package com.storm;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.Map;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

public class WordReader extends BaseRichSpout{
	private static final long serialVersionUID = -6773456320028125762L;
	private SpoutOutputCollector collector;  
    private FileReader fileReader;  
    private boolean completed = false; 

	public void nextTuple() {
		System.out.println("WR-nextTuple--");
        if (completed) {  
            try {  
                Thread.sleep(2000);  
            } catch (InterruptedException e) {  
                // Do nothing  
            }  
            return;  
        }  
        String str;  
        // Open the reader  
        BufferedReader reader = new BufferedReader(fileReader);  
        try {  
            // Read all lines  
            while ((str = reader.readLine()) != null) {  
                /** 
                 * 发射每一行，Values是一个ArrayList的实现 
                 */  
                this.collector.emit(new Values(str), str);  
            }  
        } catch (Exception e) {  
            throw new RuntimeException("Error reading tuple", e);  
        } finally {  
            completed = true;  
        }  
	}

	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        try {  
            //获取创建Topology时指定的要读取的文件路径  
            this.fileReader = new FileReader(conf.get("wordsFile").toString());  
        } catch (FileNotFoundException e) {  
            throw new RuntimeException("Error reading file ["  
                    + conf.get("wordFile") + "]");  
        }  
        //初始化发射器  
        this.collector = collector;  
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("line"));  
		
	}
}
