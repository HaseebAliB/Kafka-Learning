package kafka.course.basics;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;

public class ProducerDemo {
    private static final Logger log = LoggerFactory.getLogger(ProducerDemo.class);

    public static void main(String[] args) {
        log.info("starting producer");

        //create producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());

        //create Producer
        KafkaProducer<String,String> producer = new KafkaProducer<String, String>(properties);
        log.info("producer started successfully!!..");

        //create Producer Record
        ProducerRecord<String,String> producerRecord = new ProducerRecord<String, String>("topic1","hello worlds!!");
        log.info("producer record created successfully!!..");

        //send data- its asynch
        //producer.send(producerRecord);

        //send data with callback- its asynch
        producer.send(producerRecord, new Callback() {
            @Override
            public void onCompletion(RecordMetadata recordMetadata, Exception e) {

                if (e == null){
                    log.info("Callback Meta Data \n" +
                            "Topic: "+ recordMetadata.topic() + "\n" +
                            "Partition: "+ recordMetadata.partition() + "\n" +
                            "Offset: "+ recordMetadata.offset() + "\n" +
                            "TimeStamp: "+ new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.mmm'Z'").format(new Date(recordMetadata.timestamp())) );

                }else{
                    log.error("Failed to Send Producer Data!! ",e);
                }

            }
        });

        log.info("producer data sent successfully!!..");

        //flush - flushes all producing data when its done
        producer.flush();
        log.info("producer flushed successfully!!..");

        //close producer sending data
        producer.close();
        log.info("producer closed successfully!!..");
    }

}
