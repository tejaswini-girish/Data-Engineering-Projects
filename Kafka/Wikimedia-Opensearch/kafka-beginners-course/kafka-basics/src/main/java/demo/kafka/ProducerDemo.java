package demo.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemo {
    private static final Logger log = LoggerFactory.getLogger(ProducerDemo.class.getSimpleName());

    public static void main(String[] args) {
        log.info("Hello World");

        // create Producer properties

        Properties properties = new Properties();

        //connect to localhost
//        properties.setProperty("boostrap.servers","127.0.0.1:9092");

        // connect to conduktor playground
        properties.setProperty("security.protocol","SASL_SSL");
        properties.setProperty("sasl.jaas.config","org.apache.kafka.common.security.plain.PlainLoginModule required username=\"49VfzK1IQXTdajM48zLQwb\" password=\"eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJpc3MiOiJodHRwczovL2F1dGguY29uZHVrdG9yLmlvIiwic291cmNlQXBwbGljYXRpb24iOiJhZG1pbiIsInVzZXJNYWlsIjpudWxsLCJwYXlsb2FkIjp7InZhbGlkRm9yVXNlcm5hbWUiOiI0OVZmeksxSVFYVGRhak00OHpMUXdiIiwib3JnYW5pemF0aW9uSWQiOjczNzUyLCJ1c2VySWQiOjg1NzczLCJmb3JFeHBpcmF0aW9uQ2hlY2siOiI3NjcwM2FjYS05MjNiLTQ1MTgtYTY5NC1hNWU2MDY1NzgzOWYifX0.jhGi9dYh5DMiA3WIrwAodNQ9qmdcCXlAg1EWrrtbRCQ\";");
        properties.setProperty("sasl.mechanism","PLAIN");
        properties.setProperty("bootstrap.servers","cluster.playground.cdkt.io:9092");

        // set producer properties
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer",StringSerializer.class.getName());

        // create Producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        // create a Producer Record
        ProducerRecord<String, String> producerRecord = new ProducerRecord<>("demo_java", "hello world");
        //send data
        producer.send(producerRecord);

//        tell the producer to send all data and block until done -- synchronous
        producer.flush();

//       flush and close the producer
        producer.close();
    }
}
