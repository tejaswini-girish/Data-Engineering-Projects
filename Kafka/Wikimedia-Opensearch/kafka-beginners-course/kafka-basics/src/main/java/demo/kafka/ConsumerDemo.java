package demo.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerDemo {
    private static final Logger log = LoggerFactory.getLogger(ConsumerDemo.class.getSimpleName());

    public static void main(String[] args) {
        log.info("Kafka Consumer");

        String groupId = "my-java-application";
        String topic = "demo_java";

        // create Producer properties

        Properties properties = new Properties();

        //connect to localhost
//        properties.setProperty("boostrap.servers","127.0.0.1:9092");

        // connect to conduktor playground
        properties.setProperty("security.protocol","SASL_SSL");
        properties.setProperty("sasl.jaas.config","org.apache.kafka.common.security.plain.PlainLoginModule required username=\"49VfzK1IQXTdajM48zLQwb\" password=\"eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJpc3MiOiJodHRwczovL2F1dGguY29uZHVrdG9yLmlvIiwic291cmNlQXBwbGljYXRpb24iOiJhZG1pbiIsInVzZXJNYWlsIjpudWxsLCJwYXlsb2FkIjp7InZhbGlkRm9yVXNlcm5hbWUiOiI0OVZmeksxSVFYVGRhak00OHpMUXdiIiwib3JnYW5pemF0aW9uSWQiOjczNzUyLCJ1c2VySWQiOjg1NzczLCJmb3JFeHBpcmF0aW9uQ2hlY2siOiI3NjcwM2FjYS05MjNiLTQ1MTgtYTY5NC1hNWU2MDY1NzgzOWYifX0.jhGi9dYh5DMiA3WIrwAodNQ9qmdcCXlAg1EWrrtbRCQ\";");
        properties.setProperty("sasl.mechanism","PLAIN");
        properties.setProperty("bootstrap.servers","cluster.playground.cdkt.io:9092");

        //create consumer configs
        properties.setProperty("key.deserializer", StringDeserializer.class.getName());
        properties.setProperty("value.deserializer", StringDeserializer.class.getName());
        properties.setProperty("group.id", groupId);

        properties.setProperty("auto.offset.reset","earliest");

        //create consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        //subscribe to a topic
        consumer.subscribe(Arrays.asList(topic));

        //pull for data
        while(true){
            log.info("Polling");
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));

            for(ConsumerRecord<String, String> record: records){
                log.info("Key : "+record.key() + ", Value: " + record.value());
                log.info("Partition : "+record.partition() + ", Offset: " + record.offset());

            }
        }
    }
}
