package com.database.kafka.ExtractData;

import java.io.BufferedReader;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Properties;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.TimeUnit;

import com.database.kafka.ExtractData.Config.Topics;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import kafka.serializer.StringEncoder;

public class multiproducer extends Thread{
    private List<Topics> topicList;
    private List<DataSource> dataSources;
    private List<BufferedReader> readers;
    public multiproducer(List<Topics> topicList) {    
        this.topicList = topicList;  
    } 
    
    @Override
	public void run() {
    	final Producer producer = createProducer();
    	try {
    		dataSources = new ArrayList<DataSource>();
			final List<KeyedMessage<Integer, String>> kms = new ArrayList<KeyedMessage<Integer, String>>(); 
			for(Topics topic: topicList){
				dataSources.add(new DataSource(topic));
			}
			// 启动定时器线程，并在2秒后开始，每隔1秒执行一次定时任务
	        new Timer().schedule(new TimerTask() {
	        	SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	            @Override
	            public void run() {
	            	//producer.send(kms);
	            	int size = kms.size();
	            	kms.clear();
	            	System.out.println(df.format(new Date()) +  " 大小  ：" + size);
	            }
	        }, 2000, 1000);
	        
			boolean isRunning = true;
			
			while(isRunning){
				for(DataSource ds : dataSources){
					String tempString = null;
					isRunning = false;
					if((tempString = ds.getReader().readLine()) != null){
						isRunning = true;
						KeyedMessage<Integer, String> km = new KeyedMessage<Integer, String>(ds.getTopic().topicName, tempString);  
						kms.add(km);  
					}
				}
			}
			for(DataSource ds : dataSources){
				ds.getReader().close();
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		producer.close();
	}
    
    private Producer createProducer() {
		Properties properties = new Properties();
		properties.put("zookeeper.connect", "192.168.56.121:2181,192.168.56.122:2181,192.168.56.123:2181");// 声明zk
		properties.put("serializer.class", StringEncoder.class.getName());
		properties.put("metadata.broker.list", "192.168.56.121:9092,192.168.56.122:9092,192.168.56.123:9092");// 声明kafka  broker
		return new Producer<Integer, String>(new ProducerConfig(properties));
	}
    
	public static void main(String[] args) {
		List<Topics> topicList = new ArrayList<Topics>();
		topicList.add(Config.Topics.USERBASIC);
		topicList.add(Config.Topics.USERBEHAVIOR);
		topicList.add(Config.Topics.USEREDU);
		new multiproducer(topicList).start();
	}

}
