package auditlog;


import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Map;
import java.util.Properties;
import java.util.logging.Logger;
import java.util.Iterator;

import java.util.Arrays;
import java.util.HashMap;
import java.util.regex.Pattern;

import scala.Tuple2;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka09.ConsumerStrategies;
import org.apache.spark.streaming.kafka09.KafkaUtils;
import org.apache.spark.streaming.kafka09.LocationStrategies;




/**
 * @author jcasaletto
 * 
 * Consumes messages from input Kafka topic, performs wordcount, then outputs wordcounts to output Kafka topic
 *
 * Usage: LogAnomalyDetector <broker> <in-topic> <out-topic> <duration>
 *   <broker> is one of the servers in the kafka cluster
 *   <in-topic> is the kafka topic to consume from
 *   <out-topic> is the kafka topic to produce to
 *   <duration> is the number of milliseconds per batch
 *
 */

public final class LogAnomalyDetector{
    private static final Pattern SPACE = Pattern.compile(",");
    private static final Logger LOGGER = Logger.getLogger(LogAnomalyDetector.class.getName());
    public static void main(String[] args) {
        if (args.length < 6) {
            System.err.println("Usage: LogAnomalyDetector<broker> <in-topic> <out-topic> <threshold> <interval> <window size>");
            System.err.println("eg: LogAnomalyDetector localhost:9092 in out 5 5000 30000");
            System.exit(1);
        }

        // set variables from command-line arguments
        final String broker = args[0];
        String inTopic = args[1];
        final String outTopic = args[2];
        final Integer threshold = Integer.parseInt(args[3]);
        long interval = Long.parseLong(args[4]);
        long windowSize = Long.parseLong(args[5]);
        
        
        // hardcode some variables
        String master = "local[*]";
        String consumerGroup = "mycg";
        
        // define topic to subscribe to
        final Pattern topicPattern = Pattern.compile(inTopic, Pattern.CASE_INSENSITIVE);
    
        // set Kafka client parameters
        Map<String, Object> kafkaParams = new HashMap<String, Object>();
        kafkaParams.put("key.deserializer", 
            "org.apache.kafka.common.serialization.StringDeserializer");
        kafkaParams.put("value.deserializer", 
            "org.apache.kafka.common.serialization.StringDeserializer");
        kafkaParams.put("bootstrap.servers", broker);
        kafkaParams.put("group.id", consumerGroup);

        // initialize the streaming context
        JavaStreamingContext jssc = new JavaStreamingContext(master, "loganomalydetector", new Duration(interval));

        // pull ConsumerRecords out of the stream
        JavaInputDStream<ConsumerRecord<String, String>> messages = 
                        KafkaUtils.createDirectStream(
                        jssc,
                        LocationStrategies.PreferConsistent(),
                        ConsumerStrategies.<String, String>SubscribePattern(topicPattern, kafkaParams)
                      );
    
        // pull values out of ConsumerRecords 
        JavaDStream<String> values = messages.map(new Function<ConsumerRecord<String, String>,String>() {
           private static final long serialVersionUID = 1L;
           public String call(ConsumerRecord<String, String> record) throws Exception {
               return record.value();
            }
        }).filter(new Function<String, Boolean>() {
        	private static final long serialVersionUID = 1L;
			public Boolean call(String check1) throws Exception {
				// TODO Auto-generated method stub
				return check1.contains("USER_LOGIN");
			}
        	
		}).filter(new Function<String, Boolean>() {
			private static final long serialVersionUID = 1L;
			public Boolean call(String check2) throws Exception {
				// TODO Auto-generated method stub
				return check2.contains("failed");
			}
		});
        		
        values.print();
       
   
        // pull words out of values
        JavaDStream<String> words = values.flatMap(new FlatMapFunction<String, String>() {
            private static final long serialVersionUID = 1L;
            public Iterator<String> call(String x) {
                return Arrays.asList(x.split(" ")).iterator();
            }
        });
        words.print();
        
        // calculate word counts
        JavaPairDStream<String, Integer> wordCounts = words.mapToPair(
            new PairFunction<String, String, Integer>() {
                private static final long serialVersionUID = 1L;
                public Tuple2<String, Integer> call(String s) {
                    return new Tuple2<String, Integer>(s, 1);
                }
            }).reduceByKeyAndWindow(new Function2<Integer, Integer, Integer>() {
                private static final long serialVersionUID = 1L;
                public Integer call(Integer i1, Integer i2) {
                    return i1 + i2;
                }
            },Durations.milliseconds(windowSize), Durations.milliseconds(interval));    
        wordCounts.print();
        
        // send the wordcounts to the output stream
        wordCounts.foreachRDD(new VoidFunction<JavaPairRDD<String, Integer>>() {
            private static final long serialVersionUID = 2700738329774962618L;
            public void call(JavaPairRDD<String, Integer> rdd) throws Exception {
                rdd.foreachPartition(new VoidFunction<Iterator<Tuple2<String ,Integer>>>() {
                    private static final long serialVersionUID = -250139202220821945L;
                    public void call(Iterator<Tuple2<String, Integer>> tupleIterator) throws Exception {
                        // configure producer properties
                        Properties producerProps = new Properties();
                        producerProps.put("bootstrap.servers", broker);
                        producerProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
                        producerProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
                        
                        // instantiate the producer once per partition
                        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(producerProps); 
                              
                        // produce key value record
                        while(tupleIterator.hasNext()) {
                            Tuple2<String, Integer> tuple = tupleIterator.next();
                            if(tuple._1.toString().contains("acct") && tuple._2 > threshold){
                            ProducerRecord<String, String> producerRecord = new ProducerRecord<String, String>
                                (outTopic, tuple._1.toString(), tuple._1.toString() + "tried to log in " + tuple._2.toString() + "times.");
                            producer.send(producerRecord);
                            }
                        }
                        // close the producer per partition
                        producer.close();                              
                    }               
                });  
            }
        });
    
        // start the consumer
        jssc.start();
    
        // stay in infinite loop until terminated
        try {
            jssc.awaitTermination();
        } 
        catch (InterruptedException e) {
        }
    }
}
