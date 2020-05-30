package com.sid;

import java.io.FileInputStream;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.Properties;
import javax.swing.JOptionPane;
import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;

public class SimulateSensor implements MqttCallback {
  static MqttClient mqttclient;
  
  static String cloud_server = new String();
  
  static String cloud_topic = new String();
  
  public static void main(String[] paramArrayOfString) {
    try {
      Properties properties = new Properties();
      properties.load(new FileInputStream("src/main/resources/SimulateSensor.ini"));
      cloud_server = properties.getProperty("cloud_server");
      cloud_topic = properties.getProperty("cloud_topic");
    } catch (Exception exception) {
      System.out.println("Error reading SimulateSensor.ini file " + exception);
      JOptionPane.showMessageDialog(null, "The SimulateSensor.ini file wasn't found.", "Mongo To Cloud", 0);
    } 
    (new SimulateSensor()).connecCloud();
    (new SimulateSensor()).writeSensor();
  }
  
  public void connecCloud() {
    try {
      mqttclient = new MqttClient(cloud_server, "SimulateSensor" + cloud_topic);
      mqttclient.connect();
      mqttclient.setCallback(this);
      mqttclient.subscribe(cloud_topic);
    } catch (MqttException mqttException) {
      mqttException.printStackTrace();
    } 
  }
  
  public void writeSensor() {
    String str = new String();
    double d = 18.0D;
    LocalDate localDate = LocalDate.now();
    LocalTime localTime = LocalTime.now();
    while (true) {
      d = 18.0D;
      while (d < 50.0D) {
        localDate = LocalDate.now();
        localTime = LocalTime.now();
        str = "{\"tmp\":\"" + d + "\",\"hum\":\"" + 35.0D + "\",\"dat\":\"";
        str = str + localDate.format(DateTimeFormatter.ofPattern("dd/MM/yyyy"));
        str = str + "\",\"tim\":\"" + localTime.format(DateTimeFormatter.ofPattern("HH:mm:ss"));
        str = str + "\",\"cell\":\"" +  (Math.random()*500 +2800)  + "\",\"mov\":\"" + (Math.random() < 0.8 ? 0 : 1) + "\",\"sens\":\"eth\"}";
        d += 0.5D;
        try {
          Thread.sleep(2000L);
        } catch (InterruptedException interruptedException) {}
        publishSensor(str);
      } 
      byte b = 1;
      while (b < 10) {
        localDate = LocalDate.now();
        localTime = LocalTime.now();
        str = "{\"tmp\":\"" + d + "\",\"hum\":\"" + 35.0D + "\",\"dat\":\"";
        str = str + localDate.format(DateTimeFormatter.ofPattern("dd/MM/yyyy"));
        str = str + "\",\"tim\":\"" + localTime.format(DateTimeFormatter.ofPattern("HH:mm:ss"));
        str = str + "\",\"cell\":\"" +  (Math.random()*500 +2800)  + "\",\"mov\":\"" + (Math.random() < 0.8 ? 0 : 1) + "\",\"sens\":\"eth\"}";
        b++;
        try {
          Thread.sleep(2000L);
        } catch (InterruptedException interruptedException) {}
        publishSensor(str);
      } 
      while (d > 18.0D) {
        localDate = LocalDate.now();
        localTime = LocalTime.now();
        str = "{\"tmp\":\"" + d + "\",\"hum\":\"" + 35.0D + "\",\"dat\":\"";
        str = str + localDate.format(DateTimeFormatter.ofPattern("dd/MM/yyyy"));
        str = str + "\",\"tim\":\"" + localTime.format(DateTimeFormatter.ofPattern("HH:mm:ss"));
        str = str + "\",\"cell\":\"" +  (Math.random()*500 +2800)  + "\",\"mov\":\"" + (Math.random() < 0.8 ? 0 : 1) + "\",\"sens\":\"eth\"}";
        d -= 1.5D;
        try {
          Thread.sleep(2000L);
        } catch (InterruptedException interruptedException) {}
        publishSensor(str);
      } 
      d = 18.0D;
      localDate = LocalDate.now();
      localTime = LocalTime.now();
      str = "{\"tmp\":\"" + d + "\",\"hum\":\"" + 35.0D + "\",\"dat\":\"";
      str = str + localDate.format(DateTimeFormatter.ofPattern("dd/MM/yyyy"));
      str = str + "\",\"tim\":\"" + localTime.format(DateTimeFormatter.ofPattern("HH:mm:ss"));
      str = str + "\",\"cell\":\"" +  (Math.random()*500 +2800)  + "\",\"mov\":\"" + (Math.random() < 0.8 ? 0 : 1) + "\",\"sens\":\"eth\"}";
      try {
        Thread.sleep(2000L);
      } catch (InterruptedException interruptedException) {}
      publishSensor(str);
      d = 18.0D;
      localDate = LocalDate.now();
      localTime = LocalTime.now();
      str = "{\"tmp\":\"" + d + "\",\"hum\":\"" + 35.0D + "\",\"dat\":\"";
      str = str + localDate.format(DateTimeFormatter.ofPattern("dd/MM/yyyy"));
      str = str + "\",\"tim\":\"" + localTime.format(DateTimeFormatter.ofPattern("HH:mm:ss"));
      str = str + "\",\"cell\":\"" +  (Math.random()*500 +2800)  + "\",\"mov\":\"" + (Math.random() < 0.8 ? 0 : 1) + "\",\"sens\":\"eth\"}";
      try {
        Thread.sleep(2000L);
      } catch (InterruptedException interruptedException) {}
      publishSensor(str);
      d = 18.0D;
      localDate = LocalDate.now();
      localTime = LocalTime.now();
      str = "{\"tmp\":\"" + d + "\",\"hum\":\"" + 35.0D + "\",\"dat\":\"";
      str = str + localDate.format(DateTimeFormatter.ofPattern("dd/MM/yyyy"));
      str = str + "\",\"tim\":\"" + localTime.format(DateTimeFormatter.ofPattern("HH:mm:ss"));
      str = str + "\",\"cell\":\"" +  (Math.random()*500 +2800)  + "\",\"mov\":\"" + (Math.random() < 0.8 ? 0 : 1) + "\",\"sens\":\"eth\"}";
      try {
        Thread.sleep(2000L);
      } catch (InterruptedException interruptedException) {}
      publishSensor(str);
      d = 50.0D;
      localDate = LocalDate.now();
      localTime = LocalTime.now();
      str = "{\"tmp\":\"" + d + "\",\"hum\":\"" + 35.0D + "\",\"dat\":\"";
      str = str + localDate.format(DateTimeFormatter.ofPattern("dd/MM/yyyy"));
      str = str + "\",\"tim\":\"" + localTime.format(DateTimeFormatter.ofPattern("HH:mm:ss"));
      str = str + "\",\"cell\":\"" +  (Math.random()*500 +2800)  + "\",\"mov\":\"" + (Math.random() < 0.8 ? 0 : 1) + "\",\"sens\":\"eth\"}";
      try {
        Thread.sleep(2000L);
      } catch (InterruptedException interruptedException) {}
      publishSensor(str);
      d = 18.0D;
      localDate = LocalDate.now();
      localTime = LocalTime.now();
      str = "{\"tmp\":\"" + d + "\",\"hum\":\"" + 35.0D + "\",\"dat\":\"";
      str = str + localDate.format(DateTimeFormatter.ofPattern("dd/MM/yyyy"));
      str = str + "\",\"tim\":\"" + localTime.format(DateTimeFormatter.ofPattern("HH:mm:ss"));
      str = str + "\",\"cell\":\"" +  (Math.random()*500 +2800)  + "\",\"mov\":\"" + (Math.random() < 0.8 ? 0 : 1) + "\",\"sens\":\"eth\"}";
      try {
        Thread.sleep(2000L);
      } catch (InterruptedException interruptedException) {}
      publishSensor(str);
      d = 18.0D;
      localDate = LocalDate.now();
      localTime = LocalTime.now();
      str = "{\"tmp\":\"" + d + "\",\"hum\":\"" + 35.0D + "\",\"dat\":\"";
      str = str + localDate.format(DateTimeFormatter.ofPattern("dd/MM/yyyy"));
      str = str + "\",\"tim\":\"" + localTime.format(DateTimeFormatter.ofPattern("HH:mm:ss"));
      str = str + "\",\"cell\":\"" +  (Math.random()*500 +2800)  + "\",\"mov\":\"" + '\001' + "\",\"sens\":\"eth\"}";
      try {
        Thread.sleep(2000L);
      } catch (InterruptedException interruptedException) {}
      publishSensor(str);
      d = 18.0D;
      localDate = LocalDate.now();
      localTime = LocalTime.now();
      str = "{\"tmp\":\"" + d + "\",\"hum\":\"" + 35.0D + "\",\"dat\":\"";
      str = str + localDate.format(DateTimeFormatter.ofPattern("dd/MM/yyyy"));
      str = str + "\",\"tim\":\"" + localTime.format(DateTimeFormatter.ofPattern("HH:mm:ss"));
      str = str + "\",\"cell\":\"" +  (Math.random()*500 +2800)  + "\",\"mov\":\"" + (Math.random() < 0.8 ? 0 : 1) + "\",\"sens\":\"eth\"}";
      try {
        Thread.sleep(2000L);
      } catch (InterruptedException interruptedException) {}
      publishSensor(str);
      d = 50.0D;
      localDate = LocalDate.now();
      localTime = LocalTime.now();
      str = "{\"tmp\":\"" + d + "\",\"hum\":\"" + 35.0D + "\",\"dat\":\"";
      str = str + localDate.format(DateTimeFormatter.ofPattern("dd/MM/yyyy"));
      str = str + "\",\"tim\":\"" + localTime.format(DateTimeFormatter.ofPattern("HH:mm:ss"));
      str = str + "\",\"cell\":\"" +  (Math.random()*500 +2800)  + "\",\"mov\":\"" + (Math.random() < 0.8 ? 0 : 1) + "\",\"sens\":\"eth\"}";
      try {
        Thread.sleep(2000L);
      } catch (InterruptedException interruptedException) {}
      publishSensor(str);
      d = 50.0D;
      localDate = LocalDate.now();
      localTime = LocalTime.now();
      str = "{\"tmp\":\"" + d + "\",\"hum\":\"" + 35.0D + "\",\"dat\":\"";
      str = str + localDate.format(DateTimeFormatter.ofPattern("dd/MM/yyyy"));
      str = str + "\",\"tim\":\"" + localTime.format(DateTimeFormatter.ofPattern("HH:mm:ss"));
      str = str + "\",\"cell\":\"" +  (Math.random()*500 +2800)  + "\",\"mov\":\"x\",\"sens\":\"eth\"}";
      try {
        Thread.sleep(2000L);
      } catch (InterruptedException interruptedException) {}
      publishSensor(str);
      d = 50.0D;
      localDate = LocalDate.now();
      localTime = LocalTime.now();
      str = "{\"tmp\":\"" + d + "\",\"hum\":\"" + 35.0D + "\",\"dat\":\"";
      str = str + localDate.format(DateTimeFormatter.ofPattern("dd/MM/yyyy"));
      str = str + "\",\"tim\":\"" + localTime.format(DateTimeFormatter.ofPattern("HH:mm:ss"));
      str = str + "\",\"cell\":\"" + -20 + "\",\"mov\":\"" + (Math.random() < 0.8 ? 0 : 1) + "\",\"sens\":\"eth\"}";
      try {
        Thread.sleep(2000L);
      } catch (InterruptedException interruptedException) {}
      publishSensor(str);
      d = 50.0D;
      localDate = LocalDate.now();
      localTime = LocalTime.now();
      str = "{\"tmp\":\"" + d + "\",\"hum\":\"" + 35.0D + "\",\"dat\":\"";
      str = str + localDate.format(DateTimeFormatter.ofPattern("dd/MM/yyyy"));
      str = str + "\",\"tim\":\"" + localTime.format(DateTimeFormatter.ofPattern("HH:mm:ss"));
      str = str + "\",\"cell\":\"" +  (Math.random()*500 +2800)  + "\",\"mov\":\"" + (Math.random() < 0.8 ? 0 : 1) + "\",\"sens\":\"eth\"}";
      try {
        Thread.sleep(2000L);
      } catch (InterruptedException interruptedException) {}
      publishSensor(str);
      d = 18.0D;
      localDate = LocalDate.now();
      localTime = LocalTime.now();
      str = "{\"tmp\":\"" + d + "\",\"hum\":\"" + 35.0D + "\",\"dat\":\"";
      str = str + localDate.format(DateTimeFormatter.ofPattern("dd/MM/yyyy"));
      str = str + "\",\"tim\":\"" + localTime.format(DateTimeFormatter.ofPattern("HH:mm:ss"));
      str = str + "\",\"cell\":\"" +  (Math.random()*500 +2800)  + "\",\"mov\":\"" + (Math.random() < 0.8 ? 0 : 1) + "\",\"sens\":\"eth\"}";
      try {
        Thread.sleep(2000L);
      } catch (InterruptedException interruptedException) {}
      publishSensor(str);
    } 
  }
  
  public void publishSensor(String paramString) {
    try {
      MqttMessage mqttMessage = new MqttMessage();
      mqttMessage.setPayload(paramString.getBytes());
      mqttclient.publish(cloud_topic, mqttMessage);
    } catch (MqttException mqttException) {
      mqttException.printStackTrace();
    } 
  }
  
  public void connectionLost(Throwable paramThrowable) {}
  
  public void deliveryComplete(IMqttDeliveryToken paramIMqttDeliveryToken) {}
  
  public void messageArrived(String paramString, MqttMessage paramMqttMessage) {}
}
