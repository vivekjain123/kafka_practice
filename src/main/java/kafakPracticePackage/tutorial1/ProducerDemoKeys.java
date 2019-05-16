package kafakPracticePackage.tutorial1;


import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.*;
import org.slf4j.*;

import java.util.*;
import java.util.concurrent.*;


/**
 * Created by vivekjain on 25/03/19.
 */
public class ProducerDemoKeys {

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        //creating logger for my class
        final Logger logger = LoggerFactory.getLogger(ProducerDemoKeys.class);


        String bootstrapServers="127.0.0.1:9092";
        // create producer properties
        Properties properties = new Properties();

        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty((ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG),StringSerializer.class.getName());

       // create producer
        final KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);


        //create a producer record
        ProducerRecord<String, String> record;
        for(int i=0;i<10;i++){
            String topic ="firstTopic";
            String value = "hello world"+Integer.toString(i);
            String key = "id_"+Integer.toString(i);


            record = new ProducerRecord<String, String>(topic, value,key);

            logger.info("Key :: "+key); // log key
        //send data
        producer.send(record, new Callback() {
            public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                //executes every time a record is successfully sent or an exception is thrown
                if(e==null){
                    //record was sucessfully sent
                   logger.info("recordMetadata timestamp :: "+recordMetadata.timestamp() + "\n"+
                           "recordMetadata topic :: "+recordMetadata.topic() + "\n"+
                           "recordMetadata offset :: "+recordMetadata.offset() + "\n"+
                           "recordMetadata partition :: "+recordMetadata.partition() );
                }
                else{

                    logger.error("error while producing :: ", e);

                }
            }
        }).get();
        }
        //flush data
        producer.flush();
        //flush and close
        producer.close();
    }
}
