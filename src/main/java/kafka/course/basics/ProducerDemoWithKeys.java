package kafka.course.basics;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;

public class ProducerDemoWithKeys{
    private static final Logger log = LoggerFactory.getLogger(ProducerDemoWithKeys.class);

    public static void main(String[] args) {
        log.info("starting producer with keys");

        //create producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());

        //create Producer
        KafkaProducer<String,String> producer = new KafkaProducer<String, String>(properties);
        log.info("producer started successfully!!..");

        for (int i = 0; i<10 ; i++) {

            String topic = "topic1";
            String value = "message " + i;
            String key =  "id_"+i;

            //create Producer Record
            ProducerRecord<String, String> producerRecord =
                    new ProducerRecord<String, String>(topic,key,value);

            log.info("producer record created successfully!!..");

            //send data with callback- its asynch
            //will notice here that same keys goes to same partition, thats the purpose of using keys in producerRecord
            //for e.g keys 1,2,3 going to part0 and 4,5,6 going to part1

            producer.send(producerRecord, new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if (e == null) {
                        log.info("Callback Meta Data \n" +
                                "key: " + producerRecord.key() + "\n" +
                                "Partition: " + recordMetadata.partition() );
                    }else {
                            log.error("Failed to Send Producer Data!! ", e);
                          }
                }
            });

            log.info("producer data sent successfully!!..");
        }
        //flush - flushes all producing data when its done
        producer.flush();
        log.info("producer flushed successfully!!..");

        //close producer sending data
        producer.close();
        log.info("producer closed successfully!!..");
    }

}
