package storm;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.Map;

public class WordReader implements IRichSpout {
    private SpoutOutputCollector spoutOutputCollector;
    private FileReader fileReader;
    private boolean completed = true;
    private TopologyContext context;
    public boolean isDistributed() {
        return false;
    }

    @Override
    //open的三个参数
    //第一个 配置对象，定义topology对象是创建的；
    //第二个 TopologyContext对象，包含所有拓扑数据；
    //第三个 SpoutOutputCollector对象，它让我们发布交给bolts处理的数据。
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        try {
            this.context = topologyContext;
            this.fileReader = new FileReader(map.get("WordFile").toString());
        } catch (FileNotFoundException e) {
            throw new RuntimeException("Error reading file [" + map.get("WordFile") + "]");
        }
        this.spoutOutputCollector = spoutOutputCollector;
    }

    @Override
    public void close() {

    }

    @Override
    public void activate() {

    }

    @Override
    public void deactivate() {

    }

    //通过它向bolts发布待处理的数据。
    @Override
    public void nextTuple() {
        if(completed) {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {

            }
            return;
        }
        String str;
        BufferedReader reader = new BufferedReader(fileReader);
        try{
            while((str = reader.readLine()) != null) {
                this.spoutOutputCollector.emit(new Values(str));
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            completed = true;
        }

    }

    @Override
    public void ack(Object o) {
        System.out.println("OK: " + o);
    }

    @Override
    public void fail(Object o) {
        System.out.println("FAIL: " + o);
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
