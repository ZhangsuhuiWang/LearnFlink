package storm;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import java.util.Map;

public class WordNormalizer implements IRichBolt {
    private OutputCollector outputCollector;
    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.outputCollector = outputCollector;
    }

    @Override
    public void execute(Tuple tuple) {
        String sentence = tuple.getString(0);
        String[] words = sentence.split(" ");
        for(String word: words) {
            word = word.trim();
            if(!word.isEmpty()) {
                word = word.toLowerCase();
                outputCollector.emit(new Values(word));
            }
        }
        outputCollector.ack(tuple);
    }

    @Override
    public void cleanup() {

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("Word"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
