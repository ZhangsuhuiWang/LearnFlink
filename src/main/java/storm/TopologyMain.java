package storm;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

public class TopologyMain {
    public static void main(String[] args) throws InterruptedException {
        //定义拓扑
        TopologyBuilder topologyBuilder = new TopologyBuilder();
        topologyBuilder.setSpout("word-reader", new WordReader());
        topologyBuilder.setBolt("word-normalizer", new WordNormalizer()).shuffleGrouping("word-reader");
        topologyBuilder.setBolt("word-count", new WordCounter(), 2).fieldsGrouping("word-normalizer", new Fields("word"));

        //配置
        Config conf = new Config();
        conf.put("wordsFile", args[0]);
        conf.setDebug(false);

        //运行拓扑
        conf.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 1);
        LocalCluster localCluster = new LocalCluster();
        localCluster.submitTopology("Getting-Started-Topologie", conf, topologyBuilder.createTopology());
        Thread.sleep(1000);
        localCluster.shutdown();

    }
}
