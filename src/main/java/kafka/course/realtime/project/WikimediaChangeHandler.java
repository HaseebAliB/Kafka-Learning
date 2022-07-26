package kafka.course.realtime.project;

import com.launchdarkly.eventsource.EventHandler;
import com.launchdarkly.eventsource.MessageEvent;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WikimediaChangeHandler implements EventHandler {

    private KafkaProducer producer;
    private String topic;

    private static final Logger log = LoggerFactory.getLogger(WikimediaChangeHandler.class);

    public WikimediaChangeHandler(KafkaProducer producer, String topic) {
        this.producer = producer;
        this.topic = topic;
    }

    @Override
    public void onOpen() throws Exception {

    }

    @Override
    public void onClosed() throws Exception {
        producer.close();
    }

    @Override
    public void onMessage(String s, MessageEvent messageEvent) throws Exception {
        log.info("Receiving message: "+ messageEvent.getData());
        //async call
        producer.send(new ProducerRecord<>(topic,messageEvent.getData()));
    }

    @Override
    public void onComment(String s) throws Exception {

    }

    @Override
    public void onError(Throwable t) {
        log.error("Error at Event Handling : "+t.getMessage() , t);
    }
}
