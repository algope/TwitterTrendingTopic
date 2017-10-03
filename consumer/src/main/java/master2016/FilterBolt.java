package master2016;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.*;

public class FilterBolt extends BaseRichBolt {
    private String keyWord;
    private String STREAM_NAME;
    private String Folder;
    private int flag = -1;
    private int timeCounter = 1;
    private HashMap<String, String> counters = new HashMap<>();
    private OutputCollector collector;

    public FilterBolt(String keyWord, String streamName, String Folder) {
        this.keyWord = keyWord;
        this.STREAM_NAME = streamName;
        this.Folder = Folder;
    }

    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    public void execute(Tuple input) {
        collector.ack(input);

            String streamName = input.getSourceStreamId();
            String hashtag = input.getValues().get(4).toString();


            if (hashtag.matches(keyWord)) {
                flag++;
            }
            switch (flag) {
                case 0: {
                    flag++;
                    break;
                }
                case 1: {
                    String value = counters.get(hashtag);
                    if (value == null) {
                        counters.put(hashtag, "1");
                        break;

                    } else {
                        int counter = Integer.parseInt(value);
                        counter++;
                        String valueToMap = Integer.toString(counter);
                        counters.put(hashtag, valueToMap);
                        break;
                    }

                }
                case 2: {
                    List<Hashtag> finalList = new ArrayList<>();
                    List<String> hashtagsList = new ArrayList<>(counters.keySet());
                    List<String> countersList = new ArrayList<>(counters.values());

                    for (int p = 0; p < hashtagsList.size(); p++) {
                        Hashtag h = new Hashtag(hashtagsList.get(p), Integer.parseInt(countersList.get(p)));
                        finalList.add(h);
                    }

                    Collections.sort(finalList, new HashtagComparator());

                    String output = timeCounter + "," + streamName;
                    ArrayList<Hashtag> arrayListString = new ArrayList<>();

                    for (int l = 0; l < finalList.size() && l < 3; l++) {
                        arrayListString.add(finalList.get(l));
                    }

                    switch (arrayListString.size()) {
                        case 0: {
                            output = output + ",null,0,null,0,null,0";
                            break;
                        }
                        case 1: {
                            Hashtag outcase1 = arrayListString.get(0);
                            output = output + "," + outcase1.toString() + ",null,0,null,0";
                            break;
                        }
                        case 2: {

                            String hash = arrayListString.get(0).getHash();
                            int count = arrayListString.get(0).getCount();
                            String hash1 = arrayListString.get(1).getHash();
                            int count1 = arrayListString.get(1).getCount();

                            output = output + "," + hash + "," + count + "," + hash1 + "," + count1 + ",null,0";
                            break;
                        }
                        case 3: {

                            String hash = arrayListString.get(0).getHash();
                            int count = arrayListString.get(0).getCount();
                            String hash1 = arrayListString.get(1).getHash();
                            int count1 = arrayListString.get(1).getCount();
                            String hash2 = arrayListString.get(2).getHash();
                            int count2 = arrayListString.get(2).getCount();
                            output = output + "," + hash + "," + count + "," + hash1 + "," + count1 + "," + hash2 + "," + count2;
                            break;
                        }
                    }
                    timeCounter++;
                    flag = -1;
                    counters.clear();
                    finalList.clear();
                    hashtagsList.clear();
                    countersList.clear();
                    collector.emit(streamName + "_out", new Values(output));
                    break;
                }

            }
    }


    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream(STREAM_NAME + "_out", new Fields("FilterOutput"));

    }
}