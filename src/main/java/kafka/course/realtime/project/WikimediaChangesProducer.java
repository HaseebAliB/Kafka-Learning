package kafka.course.realtime.project;

import com.launchdarkly.eventsource.EventHandler;
import com.launchdarkly.eventsource.EventSource;
import kafka.course.basics.ProducerDemo;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class WikimediaChangesProducer {

    private static final Logger log = LoggerFactory.getLogger(ProducerDemo.class);

    public static void main(String[] args) throws Exception{
        log.info("starting wiki producer");

        //create producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());

        //settings for high throughput Producer
        properties.setProperty(ProducerConfig.LINGER_MS_CONFIG,"20");
        properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG,Integer.toString(32*1024)); // 32KB
        properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG,"snappy");

        //create Producer
        KafkaProducer<String,String> producer = new KafkaProducer<String, String>(properties);
        log.info("wiki producer started successfully!!..");

        String topic  = "wikimedia.changes";

        EventHandler handler =  new WikimediaChangeHandler(producer,topic);
        String url =  "https://stream.wikimedia.org/v2/stream/recentchange";

        EventSource.Builder eventSourceBuilder = new EventSource.Builder(handler, URI.create(url));
        EventSource eventSource = eventSourceBuilder.build();

        //start eventsource thread
        eventSource.start();

        //delay to complete executions of other threads before main ends
        TimeUnit.MINUTES.sleep(10);
    }
}
