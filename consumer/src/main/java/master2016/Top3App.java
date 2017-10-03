package master2016;


import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.kafka.spout.*;
import org.apache.storm.kafka.spout.KafkaSpoutRetryExponentialBackoff.TimeInterval;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import static org.apache.storm.kafka.spout.KafkaSpoutConfig.FirstPollOffsetStrategy.EARLIEST;

public class Top3App {

    private ArrayList<String> TOPICS = new ArrayList<>();
    private ArrayList<String> WORDS = new ArrayList<>();
    private ArrayList<String> STREAMS = new ArrayList<>();

    private String KafkaBrokerUrl;
    private String TopologyName;
    private String Folder;


    public static void main(String[] args) throws Exception {
        new Top3App().runMain(args);
    }

    private void runMain(String[] args) throws Exception {
        if (args.length == 0) {
            System.out.println("[ERROR] - Args expected (list of languages)");
            System.exit(0);
        } else {
            String langList = args[0];
            this.KafkaBrokerUrl = args[1];
            this.TopologyName = args[2];
            this.Folder = args[3];

            String[] langAndWord = langList.split(",");
            for (String langword : langAndWord) {
                String[] splitLang = langword.split(":");
                String lang = splitLang[0];
                TOPICS.add(lang);
                STREAMS.add(lang);
                WORDS.add(splitLang[1]);
            }
            submitTopologyRemoteCluster(this.TopologyName, getTopolgyKafkaSpout(), getConfig());
        }
    }

    private void submitTopologyLocalCluster(StormTopology topology, Config config) throws Exception {
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("test", config, topology);
        stopWaitingForInput();
    }

    private void submitTopologyRemoteCluster(String arg, StormTopology topology, Config config) throws Exception {
        StormSubmitter.submitTopology(arg, config, topology);
    }

    private void stopWaitingForInput() {
        try {
            System.out.println("PRESS ENTER TO STOP");
            new BufferedReader(new InputStreamReader(System.in)).readLine();
            System.exit(0);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private Config getConfig() {
        Config config = new Config();
        config.setDebug(true);
        return config;
    }

    private StormTopology getTopolgyKafkaSpout() {
        final TopologyBuilder tp = new TopologyBuilder();
        tp.setSpout("kafka_spout", new KafkaSpout<>(getKafkaSpoutConfig(getKafkaSpoutStreams())), 1);
        for (int i = 0; i < STREAMS.size(); i++) {
            tp.setBolt(STREAMS.get(i) + "_filter", new FilterBolt(WORDS.get(i), STREAMS.get(i), this.Folder)).shuffleGrouping("kafka_spout", STREAMS.get(i));
            tp.setBolt(STREAMS.get(i) + "_output", new OutputBolt(this.Folder, STREAMS.get(i))).shuffleGrouping(STREAMS.get(i) + "_filter", STREAMS.get(i) + "_out");
        }
        return tp.createTopology();
    }

    private KafkaSpoutConfig<String, String> getKafkaSpoutConfig(KafkaSpoutStreams kafkaSpoutStreams) {
        return new KafkaSpoutConfig.Builder<>(getKafkaConsumerProps(), kafkaSpoutStreams, getTuplesBuilder(), getRetryService())
                .setOffsetCommitPeriodMs(10_000)
                .setFirstPollOffsetStrategy(EARLIEST)
                .setMaxUncommittedOffsets(250)
                .build();
    }

    private KafkaSpoutRetryService getRetryService() {
        return new KafkaSpoutRetryExponentialBackoff(TimeInterval.microSeconds(500),
                TimeInterval.milliSeconds(2), Integer.MAX_VALUE, TimeInterval.seconds(10));
    }

    private Map<String, Object> getKafkaConsumerProps() {
        Map<String, Object> props = new HashMap<>();
//        props.put(KafkaSpoutConfig.Consumer.ENABLE_AUTO_COMMIT, "true");
        props.put(KafkaSpoutConfig.Consumer.BOOTSTRAP_SERVERS, KafkaBrokerUrl);
        props.put(KafkaSpoutConfig.Consumer.GROUP_ID, "kafkaSpoutTestGroup");
        props.put(KafkaSpoutConfig.Consumer.KEY_DESERIALIZER, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(KafkaSpoutConfig.Consumer.VALUE_DESERIALIZER, "org.apache.kafka.common.serialization.StringDeserializer");
        return props;
    }

    private KafkaSpoutTuplesBuilder<String, String> getTuplesBuilder() {
        System.out.println("TOPICS FOR THE TUPLES BUILDER: " + TOPICS.toString());
        String[] stockArr = new String[TOPICS.size()];
        stockArr = TOPICS.toArray(stockArr);
        return new KafkaSpoutTuplesBuilderNamedTopics.Builder<>(
                new TopicsTupleBuilder<String, String>(stockArr))
                .build();
    }

    private KafkaSpoutStreams getKafkaSpoutStreams() {
        final Fields outputFields = new Fields("topic", "partition", "offset", "key", "value");

        KafkaSpoutStreamsNamedTopics.Builder res = null;
        if (STREAMS.size() <= 0) {
            System.out.println("[ERROR] - STREAMS array cannot be empty - Especify languages");
            System.exit(0);

        } else if (STREAMS.size() > 0) {
            System.out.println(">>>>>> FIRST STREAM: " + STREAMS.get(0) + "WITH TOPIC: " + TOPICS.get(0));
            res = new KafkaSpoutStreamsNamedTopics.Builder(outputFields, STREAMS.get(0), new String[]{TOPICS.get(0)});
            for (int i = 0; i < STREAMS.size(); i++) {
                System.out.println(">>>>>> STREAM: " + STREAMS.get(i) + "WITH TOPIC: " + TOPICS.get(i));
                res.addStream(outputFields, STREAMS.get(i), new String[]{TOPICS.get(i)});
            }
            return res.build();
        }
        return null;

    }
}