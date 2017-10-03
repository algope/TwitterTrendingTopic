package org.upm.storm.tttProducer;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.Iterator;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class KafkaProducer {


    private static final String topic = "twitter-topic";

    public static void run(String consumerKey, String consumerSecret, String token, String secret, String mode, String kafkaBrokerUrl, String fileName) throws InterruptedException {

        int modeI = Integer.parseInt(mode);

        Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBrokerUrl);
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        //Creating an instance producer
        Producer<String, String> producer = new org.apache.kafka.clients.producer.KafkaProducer<String, String>(producerProps);


        if (modeI == 1) {
            System.out.println("[INFO] - mode 1 - Starting");

            try {
                FileInputStream fstream = new FileInputStream(fileName);
                BufferedReader br = new BufferedReader(new InputStreamReader(fstream));
                String strLine;

                while ((strLine = br.readLine()) != null) {
                    System.out.println("[INFO] - processing new tweet");
                    JSONObject json = parse(strLine);
                    JSONObject entities = (JSONObject) json.get("entities");
                    String lang = (String) json.get("lang");
                    JSONArray hashtags = (JSONArray) entities.get("hashtags");
                    Iterator iterator = hashtags.iterator();
                    while (iterator.hasNext()) {
                        System.out.println("[INFO] - sending hashtag to kafka");
                        JSONObject hashtag = (JSONObject) iterator.next();
                        String hash = (String) hashtag.get("text");
                        System.out.println("[INFO] - hashtag: #"+hash+" language: "+lang);
                        String res = producer.send(new ProducerRecord<String, String>(lang, hash)).toString();
                        System.out.println("[INFO] - Sending status: "+res);
                        System.out.println("[INFO] - hashtag: #"+hash+"  language: "+lang+" ->sent");


                    }
                }
                br.close();
                producer.close();

            } catch (Exception e) {
                e.printStackTrace();
            }


        } else if (modeI == 2) {
            System.out.println("[INFO] - mode 2 - Starting");
            BlockingQueue<String> queue = new LinkedBlockingQueue<String>(10000);
            StatusesFilterEndpoint endpoint = new StatusesFilterEndpoint();
            endpoint.trackTerms(Lists.newArrayList("#"));
            Authentication auth = new OAuth1(consumerKey, consumerSecret, token, secret);
            Client client = new ClientBuilder().hosts(Constants.STREAM_HOST).endpoint(endpoint).authentication(auth).processor(new StringDelimitedProcessor(queue)).build();
            client.connect();
            System.out.println("[INFO] - connected to twitter");

            while (true) {
                System.out.println("[INFO] - processing new tweet");
                JSONObject json = parse(queue.take());
                JSONObject entities = (JSONObject) json.get("entities");
                String lang = (String) json.get("lang");
                JSONArray hashtags = (JSONArray) entities.get("hashtags");
                Iterator iterator = hashtags.iterator();

                while (iterator.hasNext()) {
                    System.out.println("[INFO] - sending hashtag to kafka");
                    JSONObject hashtag = (JSONObject) iterator.next();
                    String hash = (String) hashtag.get("text");
                    System.out.println("[INFO] - hashtag: #"+hash+" language: "+lang);
                    producer.send(new ProducerRecord<String, String>(lang, hash));
                    System.out.println("[INFO] - hashtag: #"+hash+"  language: "+lang+" ->sent");
                }
            }

        }

    }

    private static JSONObject parse(String s) {
        JSONParser parser = new JSONParser();
        try {
            Object obj = parser.parse(s);
            return (JSONObject) obj;

        } catch (Exception e) {
            System.out.println("[ERROR] - parser exception - " + e);
            return null;
        }
    }

}