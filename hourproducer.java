package com.database.kafka.ExtractData;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import com.database.kafka.ExtractData.Config.Topics;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import kafka.serializer.StringEncoder;

public class hourproducer extends Thread {
	private Topics topic;
    private DataSource dataSource;
    public hourproducer(Topics topic) throws FileNotFoundException{    
        this.dataSource = new DataSource(topic);
        this.topic = topic;  
    } 
    
    private BufferedReader readers;
    
	@Override
	public void run() {
		final Producer producer = createProducer();
		SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    	try {
			final List<KeyedMessage<Integer, String>> kms = new ArrayList<KeyedMessage<Integer, String>>(); 
			String tempString = null;
			while((tempString = dataSource.getReader().readLine()) != null){
				producer.send(new KeyedMessage<Integer, String>(dataSource.getTopic().topicName, tempString));
				 System.out.println(df.format(new Date()) + "  " + tempString);
				try {
					TimeUnit.SECONDS.sleep(5); //使用5秒钟代替1小时
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
			System.out.println(kms.size());
			dataSource.getReader().close();
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

	public static void main(String[] args) throws FileNotFoundException {
		new hourproducer(Config.Topics.USERBASIC).start();
	}


}
