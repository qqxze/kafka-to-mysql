package com.database.kafka.ExtractData;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;    
import java.util.List;    
import java.util.Map;    
import java.util.Properties;    
  
import kafka.consumer.Consumer;    
import kafka.consumer.ConsumerConfig;    
import kafka.consumer.ConsumerIterator;    
import kafka.consumer.KafkaStream;    
import kafka.javaapi.consumer.ConsumerConnector;    
import com.database.kafka.ExtractData.Config.Topics;

public class kafkaConsumer extends Thread{    
  
    private Topics type;

    public kafkaConsumer(Topics type){    
        super();    
        this.type = type;  
    }    
    @Override    
    public void run() {
    	 Connection conn = null;
         Statement stmt = null;
         try{
             Class.forName("com.mysql.jdbc.Driver");
             conn = DriverManager.getConnection(Config.DB_URL,Config.USER,Config.PASS);
             
             PreparedStatement pstmt = conn.prepareStatement(type.insertSQL);  
             stmt = conn.createStatement();
             ConsumerConnector consumer = createConsumer();    
             Map<String, Integer> topicCountMap = new HashMap<String, Integer>();    
             topicCountMap.put("behave", 1); 
	         Map<String, List<KafkaStream<byte[], byte[]>>>  messageStreams = consumer.createMessageStreams(topicCountMap);    
	         KafkaStream<byte[], byte[]> stream = messageStreams.get("behave").get(0);// 閼惧嘲褰囧В蹇旑偧閹恒儲鏁归崚鎵畱鏉╂瑤閲滈弫鐗堝祦   
	         int size=stream.size();
             System.out.println(size);
	         ConsumerIterator<byte[], byte[]> iterator =  stream.iterator();    
 	         while(iterator.hasNext()){    
	              String message = new String(iterator.next().message());
	              //System.out.println(message);
	              System.out.println("从kafka获取数据: " + message);
	              String[] info = message.split("\001");
	              for (int i = 0; i < info.length; i++) {  
	                  pstmt.setString(i + 1,  info[i]);  
		              System.out.println(info[i]);
	              }  
	              pstmt.executeUpdate();
	         }    
 	         
             stmt.close();
             conn.close();
         }catch(SQLException se){
             se.printStackTrace();
         }catch(Exception e){
             e.printStackTrace();
         }finally{
             try{
                 if(stmt!=null) stmt.close();
             }catch(SQLException se2){
             }
             try{
                 if(conn!=null) conn.close();
             }catch(SQLException se){
                 se.printStackTrace();
             }
         }
    }    
    
    private ConsumerConnector createConsumer() {    
        Properties properties = new Properties();    
        properties.put("zookeeper.connect", "192.168.56.121:2181,192.168.56.122:2181,192.168.56.123:2181");//婢圭増妲憐k    
        properties.put("group.id", "group2");
        return Consumer.createJavaConsumerConnector(new ConsumerConfig(properties));    
     }    
        
    public static void main(String[] args) {    
        new kafkaConsumer(Config.Topics.USERBEHAVIOR).start();     
//        new kafkaConsumer(Config.Topics.USERBEHAVIOR).start();
//        new kafkaConsumer(Config.Topics.USERBEHAVIOR).start(); 
//        new kafkaConsumer(Config.Topics.USERBEHAVIOR).start(); 
        
    }         
}  