package auditlog;

import java.io.File;
import java.util.Properties;

import org.apache.commons.io.input.Tailer;
import org.apache.commons.io.input.TailerListener;
import org.apache.commons.io.input.TailerListenerAdapter;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class Log2Topic extends TailerListenerAdapter {
	public static String topic;
	public static KafkaProducer<String, String> producer;
	public static void main(String args[]){
		String brokerName = args[2];
		String brokerPort = args[3];
		configureProducer(brokerName, brokerPort);
		topic = args[1];
		
		TailerListener tListener = new TailerListenerAdapter(){
			@Override
			public void handle(String str){
				ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, str);
				producer.send(record);
				
				System.out.println("Sent to topic: "+ str);
				
			}
		};
		Tailer tailer = Tailer.create(new File(args[0]), tListener, 5000);
		//infinite loop
		while(true){}	
				
	}
	
	public static void configureProducer(String brokername, String brokerport){
		String s = brokername +":"+ brokerport;
		Properties props = new Properties();
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("bootstrap.servers", s);
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		producer = new KafkaProducer<String, String>(props);
	}

}
