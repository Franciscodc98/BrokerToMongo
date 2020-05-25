package com.sid;

import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.json.JSONObject;

import com.mongodb.*;
import com.mongodb.util.JSON;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.io.*;
import javax.swing.*;

@SuppressWarnings("deprecation")
public class CloudToMongo implements MqttCallback {

	private MqttClient mqttclient;
	private static MongoClient mongoClient;

	private static DB db;
	private static DBCollection mongocol;

	private static String cloud_server;
	private static String cloud_topic;
	private static String mongo_host;
	private static String mongo_database;
	private static String mongo_collection;

	public static void main(String[] args) {
		try {

			Properties p = new Properties();
			p.load(new FileInputStream("src/main/resources/cloudToMongo.ini"));
			cloud_server = p.getProperty("cloud_server");
			cloud_topic = p.getProperty("cloud_topic");
			mongo_host = p.getProperty("mongo_host");
			mongo_database = p.getProperty("mongo_database");
			mongo_collection = p.getProperty("mongo_collection");

		} catch (Exception e) {
			System.out.println("Error reading CloudToMongo.ini file " + e);
			JOptionPane.showMessageDialog(null, "The CloudToMongo.inifile wasn't found.", "CloudToMongo",
					JOptionPane.ERROR_MESSAGE);
		}

		new CloudToMongo().connecCloud();
		new CloudToMongo().connectMongo();
	}

	public void connecCloud() {
		try {
			int i = new Random().nextInt(100000);
			mqttclient = new MqttClient(cloud_server, "CloudToMongo_" + String.valueOf(i) + "_" + cloud_topic);
			mqttclient.connect();
			mqttclient.setCallback(this);
			mqttclient.subscribe(cloud_topic);
		} catch (MqttException e) {
			e.printStackTrace();
		}
	}

	public void connectMongo() {
		mongoClient = new MongoClient(new MongoClientURI(mongo_host));
		db = mongoClient.getDB(mongo_database);
		mongocol = db.getCollection(mongo_collection);
	}

	@Override
	public void messageArrived(String topic, MqttMessage c) throws Exception {
		try {
			DBObject document_json;

			if (!checkFormat(c)) {
				document_json = fixFormat(c);
			} else {
				document_json = (DBObject) JSON.parse(c.toString());
			}

			if (verifyDuplicated(document_json)) {
				mongocol.insert(document_json);
				System.out.println("Inserted: " + document_json.toString());
			} else {
				System.out.println("Duplicater: " + document_json.toString());
			}

		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * 
	 * This method will fix the message format, it will
	 * remove extra special characters and fix wrong Keys
	 * 
	 * @param c
	 * @return
	 */
	private DBObject fixFormat(MqttMessage c) {
		String rawMessage = c.toString();
		rawMessage = rawMessage.replace("\"\"", "\"");
		
		DBObject aux = (DBObject) JSON.parse(rawMessage);
		Set<String> keys = aux.keySet();
		List<String> wrongKeys = new ArrayList<String>();
		List<String> fixedKeys = new ArrayList<String>();
		Pattern p = Pattern.compile("[!@#$%^&*(),.?\":{}|<>]");
		for (String k : keys) {
			Matcher m = p.matcher(k);
			if (m.find()) {
				wrongKeys.add(k);
				k = k.replaceAll("[^a-zA-Z0-9]", "");
				fixedKeys.add("\"" + k + "\"");
			}
		}

		int count = 0;
		for (String k : wrongKeys) {
			rawMessage = rawMessage.replace(k, fixedKeys.get(count));
			count++;
		}
		
		if(!rawMessage.contains("\"cell\"")) {
			rawMessage = rawMessage.replaceFirst("\\{", "\\{\"cell\":0, ");
		}
		
		
		
		
		return (DBObject) JSON.parse(rawMessage);
	}

	/**
	 * This method will verify if an JSON object is on the database already, or not
	 * if it is, will return false if not, will return true
	 * 
	 * @param DBObject document_json
	 * @return
	 */
	private boolean verifyDuplicated(DBObject document_json) {
		DBObject aux = document_json;
		aux.removeField("_id");
		Cursor cursor = mongocol.find(aux);

		return !cursor.hasNext();
	}

	/**
	 * This method will check if the message format is valid to be parsed to JSON if
	 * it is a valid format, will return true if not, return false
	 * 
	 * @param MqttMessage c
	 * @return boolean
	 */
	private boolean checkFormat(MqttMessage c) {
		try {
			JSON.parse(c.toString());
			return true;
		} catch (Exception e) {
			return false;
		}
	}

	@Override
	public void connectionLost(Throwable cause) {
	}

	@Override
	public void deliveryComplete(IMqttDeliveryToken token) {
	}
}