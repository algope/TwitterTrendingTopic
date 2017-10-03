package master2016;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Map;

public class OutputBolt extends BaseRichBolt {

    private OutputCollector collector;
    private String folder;
    private String lang;

    public OutputBolt(String folder, String lang) {
        this.folder = folder;
        this.lang = lang;
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }

    public void prepare(Map map, TopologyContext topologyContext, OutputCollector collector) {
        this.collector = collector;
    }

    public void execute(Tuple tuple) {
        collector.ack(tuple);
        String inputString = tuple.getValues().toString();

        try (FileWriter fw = new FileWriter(this.folder + lang + "_09.log", true);
        BufferedWriter bw = new BufferedWriter(fw);
        PrintWriter out = new PrintWriter(bw)) {
            out.println(inputString);

            out.flush();
            bw.flush();
            fw.flush();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
