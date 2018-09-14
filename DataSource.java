package com.database.kafka.ExtractData;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;

import com.database.kafka.ExtractData.Config.Topics;

public class DataSource {
	
	private Topics topic;
	private BufferedReader reader = null;

	public DataSource(Topics topic) throws FileNotFoundException{
		this.topic = topic;
		//File file = new File("./dataset/" + topic.topicName + "/" + topic.topicName);
		File file = new File("./" + topic.topicName);

		//File file = new File("./datas");
		this.reader = new BufferedReader(new FileReader(file));
	}
	
	public BufferedReader getReader(){
		return this.reader;
	}
	
	public Topics getTopic(){
		return this.topic;
	}
}
