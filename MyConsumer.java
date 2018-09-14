package com.database.kafka.ExtractData;

import java.util.HashMap; 
import java.util.List;   
import java.util.Map;   
import java.util.Properties;   
     
import kafka.consumer.ConsumerConfig;   
import kafka.consumer.ConsumerIterator;   
import kafka.consumer.KafkaStream;   
import kafka.javaapi.consumer.ConsumerConnector;  
   
public class MyConsumer extends Thread{ 
        //����������
        private final ConsumerConnector consumer;   
        //Ҫ���ѵĻ���
        private final String topic;   
     
        public MyConsumer(String topic) {   
            consumer =kafka.consumer.Consumer   
                    .createJavaConsumerConnector(createConsumerConfig());   
            this.topic =topic;   
        }   
     
    //���������Ϣ
    private static ConsumerConfig createConsumerConfig() {   
        Properties props = new Properties();   
//        props.put("zookeeper.connect","localhost:2181,10.XX.XX.XX:2181,10.XX.XX.XX:2181");
        //����Ҫ���ӵ�zookeeper��ַ��˿�
        //The ��zookeeper.connect�� string identifies where to find once instance of Zookeeper in your cluster.
        //Kafka uses ZooKeeper to store offsets of messages consumed for a specific topic and partition by this Consumer Group
        props.put("zookeeper.connect","172.31.42.214:2181,172.31.42.152:2181,172.31.42.87:2181");
        
        //����zookeeper����id (The ��group.id�� string defines the Consumer Group this process is consuming on behalf of.)
        props.put("group.id", "0");
        
        //����zookeeper���ӳ�ʱ���
        //The ��zookeeper.session.timeout.ms�� is how many milliseconds Kafka will wait for 
        //ZooKeeper to respond to a request (read or write) before giving up and continuing to consume messages.
        props.put("zookeeper.session.timeout.ms","10000"); 
 
        //The ��zookeeper.sync.time.ms�� is the number of milliseconds a ZooKeeper ��follower�� can be behind the master before an error occurs.
        props.put("zookeeper.sync.time.ms", "200");

        //The ��auto.commit.interval.ms�� setting is how often updates to the consumed offsets are written to ZooKeeper. 
        //Note that since the commit frequency is time based instead of # of messages consumed, if an error occurs between updates to ZooKeeper on restart you will get replayed messages.
        props.put("auto.commit.interval.ms", "1000");
        return new ConsumerConfig(props);   
    }

    @Override
    public void run(){
        
        Map<String,Integer> topickMap = new HashMap<String, Integer>();   
        topickMap.put(topic, 1);   
        Map<String, List<KafkaStream<byte[],byte[]>>>  streamMap =consumer.createMessageStreams(topickMap);   
        
        KafkaStream<byte[],byte[]>stream = streamMap.get(topic).get(0);  
        //System.out.println(stream);   

        ConsumerIterator<byte[],byte[]> it =stream.iterator();   
        System.out.println("*********Results********");   
        while(true){   
            if(it.hasNext()){ 
            	System.out.print("prepare to get data");
                //��ӡ�õ�����Ϣ   
                System.err.println(Thread.currentThread()+" get data:" +new String(it.next().message()));   
            } 
            try {   
            	//System.out.print("111wwf");
                Thread.sleep(1000);   
            } catch (InterruptedException e) {   
                e.printStackTrace();   
            }   
        }   
    }  
    
    
    public static void main(String[] args) {   
        MyConsumer consumerThread = new MyConsumer("mykafka2");   
        consumerThread.start();   
    }   
}
