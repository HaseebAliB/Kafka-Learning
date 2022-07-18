package kafka.course.basics;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerDemo {
    private static final Logger log = LoggerFactory.getLogger(ProducerDemo.class);

    public static void main(String[] args) {
        log.info("Starting consumer!");
        Properties properties = new Properties();

        //consumer will read as part of this group
        String groupId = "Group1";

        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"127.0.0.1:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,groupId);
        //read from the initial message offset
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");

        //create consumer
        KafkaConsumer<String,String> consumer = new KafkaConsumer<String, String>(properties) ;

        //get a reference to current thread
        Thread mainThread = Thread.currentThread();

        //adding shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(){
            @Override
            public void run() {
                //super.run();
                log.info("Detected shutdown go for consumer wakeup now!");
                consumer.wakeup(); // polling will throw exception when consumer is woken up.

                //join the main thread to allow the execution of the code in main thread
                try{
                    mainThread.join();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });

        try {
            //subscribe consumer to topic(s)
            consumer.subscribe(Arrays.asList("topic1"));

            //poll for new Data
            while (true) {
                log.info("polling data");

                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000)); // delay between each poll

                for (ConsumerRecord<String, String> record : records) {
                    log.info("Key: " + record.key() + " Value: " + record.value());
                    log.info("Partition: " + record.partition() + " Offset: " + record.offset());
                }

            }
        }catch(WakeupException e){
            log.info("Consumer Woke Up. This is wakeup Exception!");
        }catch (Exception e){
            log.error("Unexpected exception",e);
        }finally {
            consumer.close(); // this will also commit offsets if need be.
            log.info("Consumer shut down gracefully!");
        }


    }

}
