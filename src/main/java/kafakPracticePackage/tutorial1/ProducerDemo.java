package kafakPracticePackage.tutorial1;


import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;


/**
 * Created by vivekjain on 25/03/19.
 */
public class ProducerDemo {
    public static void main(String[] args) {

        String bootstrapServers="127.0.0.1:9092";
        // create producer properties
        Properties properties = new java.util.Properties();

        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty((ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG),StringSerializer.class.getName());

       // create producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        //create a producer record
        ProducerRecord<String, String> record;
        record = new ProducerRecord<String, String>("firstTopic", "hello kafka3");
        //send data
        producer.send(record);
        //flush data
        producer.flush();
        //flush and close
        producer.close();
    }
}
