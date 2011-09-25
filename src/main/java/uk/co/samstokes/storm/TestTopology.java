package uk.co.samstokes.storm;

import backtype.storm.Config;

import backtype.storm.LocalCluster;

import backtype.storm.topology.TopologyBuilder;

import uk.co.samstokes.storm.scheme.JSONScheme;
import uk.co.samstokes.storm.spout.AMQPSpout;

public class TestTopology {
    public static final String TEST_TOPOLOGY_NAME = "amqpTest";

    public static void main (String[] args) {
        final TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout(1, new AMQPSpout(
              args[0],
              Integer.parseInt(args[1]),
              args[2],
              args[3],
              args[4],
              args[5],
              args[6],
              new JSONScheme()
              ));

        final Config config = new Config();
        config.setDebug(true);

        final LocalCluster cluster = new LocalCluster();
        cluster.submitTopology(TEST_TOPOLOGY_NAME, config, builder.createTopology());

        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                cluster.killTopology(TEST_TOPOLOGY_NAME);
                cluster.shutdown();
            }
        });
    }
}
