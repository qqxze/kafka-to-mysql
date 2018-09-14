package com.database.kafka.ExtractData;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

public class producerDemo {
    public static void main(String[] args) {   
        Properties props = new Properties();   
        props.setProperty("metadata.broker.list","192.168.56.121:9092,192.168.56.122:9092,192.168.56.123:9092"); 
		props.put("zookeeper.connect", "192.168.56.121:2181,192.168.56.122:2181,192.168.56.123:2181");// 声明zk

        props.setProperty("serializer.class","kafka.serializer.StringEncoder");   
        //props.put("request.required.acks","1");   
        ProducerConfig config = new ProducerConfig(props);   
        //创建生产这对象
        Producer<String, String> producer = new Producer<String, String>(config);
        //生成消息
        
        File file = new File("./userBehavior");
        BufferedReader reader = null;
        KeyedMessage<String, String> data = new KeyedMessage<String, String>("mykafka","the new message");
		SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

        try {   
//        	reader = new BufferedReader(new FileReader(file));
//			String tempString = null;
        	int i=0;
			//while ((tempString = reader.readLine()) != null) {
        	while (i<100) {
//				KeyedMessage<String, String> data = new KeyedMessage<String, String>("behave",tempString);
				producer.send(data);
	            System.out.println(df.format(new Date()) + "  " +"the new message");
				try {
					TimeUnit.SECONDS.sleep(1);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
				i++;
			}
			reader.close();
        } catch (Exception e) {   
            e.printStackTrace();   
        }   
        producer.close();   
    }

}
