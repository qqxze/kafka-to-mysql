package com.database.kafka.ExtractData;

import java.io.BufferedReader;
import java.io.File;
import java.text.SimpleDateFormat;
import java.io.FileReader;
import java.util.Date;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

public class simpleproducer {
    public static void main(String[] args) {   
        Properties props = new Properties();   
        props.setProperty("metadata.broker.list","172.31.42.214:9092,172.31.42.152.122:9092,172.31.42.87:9092");
		props.put("zookeeper.connect", "172.31.42.214:2181,172.31.42.152:2181,172.31.42.87:2181");// 声明zk

        props.setProperty("serializer.class","kafka.serializer.StringEncoder");   
        //props.put("request.required.acks","1");   
        ProducerConfig config = new ProducerConfig(props);   
        //创建生产这对象
        Producer<String, String> producer = new Producer<String, String>(config);
        //生成消息
        
        File file = new File("./userBehavior");
        BufferedReader reader = null;
        //KeyedMessage<String, String> data = new KeyedMessage<String, String>("mykafka","the new message");
		SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

        try {   
        	reader = new BufferedReader(new FileReader(file));
			String tempString = null;
			while ((tempString = reader.readLine()) != null) {
				KeyedMessage<String, String> data = new KeyedMessage<String, String>("behave",tempString);
				producer.send(data);
	            System.out.println(df.format(new Date()) + "  " +tempString);
				try {
					TimeUnit.SECONDS.sleep(1);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
			reader.close();
        } catch (Exception e) {   
            e.printStackTrace();   
        }   
        producer.close();   
    }

}
