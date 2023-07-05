package demo.kafka;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerDemoCooperative {
    private static final Logger log = LoggerFactory.getLogger(ConsumerDemoCooperative.class.getSimpleName());

    public static void main(String[] args) {
        log.info("Kafka Consumer");

        String groupId = "my-java-application";
        String topic = "demo_java";

        // create Producer properties

        Properties properties = new Properties();

        //connect to localhost
//        properties.setProperty("boostrap.servers","127.0.0.1:9092");

        // connect to conduktor playground
        properties.setProperty("security.protocol", "SASL_SSL");
        properties.setProperty("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"49VfzK1IQXTdajM48zLQwb\" password=\"eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJpc3MiOiJodHRwczovL2F1dGguY29uZHVrdG9yLmlvIiwic291cmNlQXBwbGljYXRpb24iOiJhZG1pbiIsInVzZXJNYWlsIjpudWxsLCJwYXlsb2FkIjp7InZhbGlkRm9yVXNlcm5hbWUiOiI0OVZmeksxSVFYVGRhak00OHpMUXdiIiwib3JnYW5pemF0aW9uSWQiOjczNzUyLCJ1c2VySWQiOjg1NzczLCJmb3JFeHBpcmF0aW9uQ2hlY2siOiI3NjcwM2FjYS05MjNiLTQ1MTgtYTY5NC1hNWU2MDY1NzgzOWYifX0.jhGi9dYh5DMiA3WIrwAodNQ9qmdcCXlAg1EWrrtbRCQ\";");
        properties.setProperty("sasl.mechanism", "PLAIN");
        properties.setProperty("bootstrap.servers", "cluster.playground.cdkt.io:9092");

        //create consumer configs
        properties.setProperty("key.deserializer", StringDeserializer.class.getName());
        properties.setProperty("value.deserializer", StringDeserializer.class.getName());
        properties.setProperty("group.id", groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.setProperty("partition.assignment.strategy", CooperativeStickyAssignor.class.getName());

        //create consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        //get a reference to the main Thread
        final Thread mainThread = Thread.currentThread();

        //adding the shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                log.info("Detected a shutdown, lets exit by calling consumer.wakeup()...");
                consumer.wakeup();

                //join the main thread to allow the execution of the code in the main thread
                try {
                    mainThread.join();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });


        try {
            //subscribe to a topic
            consumer.subscribe(Arrays.asList(topic));

            //pull for data
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

                for (ConsumerRecord<String, String> record : records) {
                    log.info("Key : " + record.key() + ", Value: " + record.value());
                    log.info("Partition : " + record.partition() + ", Offset: " + record.offset());
                }
            }
        } catch (WakeupException e){
            log.info("Consumer is starting to shut down");
        } catch (Exception e){
            log.error("Unexpected Exception",e);
        }
        finally {
            consumer.close();
            log.info("Consumer is now graceful shutdown");
        }
    }
}

