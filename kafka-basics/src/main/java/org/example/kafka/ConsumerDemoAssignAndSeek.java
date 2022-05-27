package org.example.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

public class ConsumerDemoAssignAndSeek {

    static final Logger logger = LoggerFactory.getLogger(ConsumerDemoAssignAndSeek.class.getName());

    public static void main(String[] args) {

        String bootstrapServer = "localhost:9092";
        String topic = "first_topic";

        // set the properties
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // Declare the consumer class
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);

        Long offsetToReadFrom = 15L;
        int partitionToReadFrom = 0;
        TopicPartition partition = new TopicPartition(topic, partitionToReadFrom);

        // assign the consumer
        consumer.assign(List.of(partition));

        // seek the data
        consumer.seek(partition, offsetToReadFrom);

        boolean keepOnReading = true;
        int keepReadingUntil = 5;
        int currentReadingPosition = 0;

        // poll for new data
        while (keepOnReading) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

            for (ConsumerRecord<String, String> record : records) {
                currentReadingPosition++;
                logger.info("Key: " + record.key() + ", Value: " + record.value());
                logger.info("Partition: " + record.partition() + ", Offset: " + record.offset());
                if (currentReadingPosition >= keepReadingUntil) {
                    keepOnReading = false;
                    break;
                }
            }
        }
    }
}
