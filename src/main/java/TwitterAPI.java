import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class TwitterAPI {

    public static void main(String[] args) throws Exception{

        // Create a new KafkaProducerAPI object
        KafkaProducerAPI kafkaProducerAPI = new KafkaProducerAPI();

        // Twitter keys and tokens
        String consumerAPIKey = "";
        String consumerAPISecretKey = "";
        String accessToken = "";
        String accessTokenSecret = "";

        // Max read tweet count
        int maxReadCount = 1000;

        // Define our endpoint
        StatusesFilterEndpoint endpoint = new StatusesFilterEndpoint();
        endpoint.trackTerms(Lists.newArrayList("kafka", "blockchain"));

        // Authentication auth = new com.twitter.hbc.httpclient.auth.BasicAuth(username, password);
        Authentication auth = new OAuth1(consumerAPIKey, consumerAPISecretKey, accessToken, accessTokenSecret);

        // Create an appropriately sized blocking queue
        BlockingQueue<String> queue = new LinkedBlockingQueue<String>(10000);

        // Create a new Client. By default gzip is enabled.
        Client client = new ClientBuilder()
                .hosts(Constants.STREAM_HOST)
                .endpoint(endpoint)
                .authentication(auth)
                .processor(new StringDelimitedProcessor(queue))
                .build();

        // Establish a connection
        client.connect();

        // Get tweets and send to Kafka Server
        for(int i = 0; i < maxReadCount; i++){
            String tweet = queue.take();
            kafkaProducerAPI.send(tweet);
            System.out.println("***** Send to Kafka Server *****");
            System.out.println(tweet);
        }

        // Destroy Client
        client.stop();

    }

}