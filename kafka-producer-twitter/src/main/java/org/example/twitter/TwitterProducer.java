package org.example.twitter;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import twitter4j.*;
import twitter4j.conf.ConfigurationBuilder;

import java.util.*;

public class TwitterProducer {

    static final Logger logger = LoggerFactory.getLogger(TwitterProducer.class.getName());

    public TwitterProducer() {
    }

    public static Twitter getTwitterinstance() {

        ConfigurationBuilder cb = new ConfigurationBuilder();
        cb.setDebugEnabled(true)
                .setOAuthConsumerKey("2EFItzOnRN2pfiztn4agKB3bC")
                .setOAuthConsumerSecret("uM3ejIzLWUQEfWp3pKkvoxd09XJaSH9llLrkC5ymM0PBq8vCc5")
                .setOAuthAccessToken("1035024576315441152-IeP3sBkyEPsuaJeWivqM2Awj9qsDgq")
                .setOAuthAccessTokenSecret("TrDP5rMNEOHp95wqaR2vw6WBBjof3S2ihkgdbRDAe46OX");
        TwitterFactory tf = new TwitterFactory(cb.build());
        Twitter twitter = tf.getInstance();
        return twitter;

    }

    private KafkaProducer<String, String> createKafkaProducer() {

        // declare the properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // create a safe producer
        properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");
        properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        properties.setProperty(ProducerConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE));
        properties.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5");

        // high throughput producer (at the expense of latency and CPU usage)
        properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
        properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "20");
        properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(1024 * 32)); // 32kb batch size

        // create the producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);
        return producer;
    }

    public static void main(String[] args) throws TwitterException {
        new TwitterProducer().run();
    }

    private void run() {
        // create a twitter client
        Twitter twitter = getTwitterinstance();
        // create a search topic
        Query query = new Query("bitcoin");

        // create a kafka producer
        KafkaProducer<String, String> producer = createKafkaProducer();

        // add a shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(new TimerTask() {
            @Override
            public void run() {
                logger.info("Stopping application");
                logger.info("Shutting down client from twitter");
                logger.info("Shutting down twitter client successful");
                logger.info("Shutting down Kafka");
                producer.close();
                logger.info("Shutting down kafka successful");
            }
        }));

        // loop to send tweets to kafka
        // on a different thread, or multiple different threads....
        Timer timer = new Timer();
        timer.schedule(new TimerTask() {
            @Override
            public void run() {
                try {
                    QueryResult result = twitter.search(query);
                    Status status = result.getTweets().get(0);

//                    String message = "{\"User\":" + status.getUser().getName() +
//                            ", Username-->> " + status.getUser().getScreenName() +
//                            ", Status-->> " + status.getText().replace("\n", "");

                    Map<String, String> hashMap = new HashMap<>();
                    hashMap.put("User", status.getUser().getName());
                    hashMap.put("Username", status.getUser().getScreenName());
                    hashMap.put("Status", status.getText().replace("\n", ""));
                    String message = new JSONObject(hashMap).toString();

                    logger.info(message);
                    producer.send(new ProducerRecord<>("twitter_tweets", null, message), new Callback() {
                        @Override
                        public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                            if (e != null) {
                                logger.error("Something bad happened", e);
                            }
                        }
                    });
                } catch (TwitterException e) {
                    throw new RuntimeException(e);
                }
            }
        }, 0, 1);

    }
}
