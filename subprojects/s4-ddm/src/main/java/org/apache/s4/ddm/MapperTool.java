/**
 * Map PE to clusters. 
 */
package org.apache.s4.ddm;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.I0Itec.zkclient.exception.ZkNoNodeException;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Level;
import org.apache.s4.comm.topology.ClusterNode;
import org.apache.s4.comm.topology.ZNRecord;
import org.apache.s4.comm.topology.ZkClient;
import org.apache.s4.core.util.AppConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.beust.jcommander.IStringConverter;
import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.google.common.collect.Maps;

/**
 * A tool to modify stream-cluster mapping in ZooKeeper
 */
public class MapperTool {

    static Logger logger = LoggerFactory.getLogger(MapperTool.class);

    private static final String NONE = "--";

    /**
     * @param args
     */
    public static void main(String[] args) throws Exception {
        // configure log4j for Zookeeper
        BasicConfigurator.configure();
        org.apache.log4j.Logger.getLogger("org.apache.zookeeper").setLevel(Level.ERROR);
        org.apache.log4j.Logger.getLogger("org.I0Itec").setLevel(Level.ERROR);

        List<String> realArgs = new ArrayList<String>();
        if (args.length < 1) {
            System.out.println("Not enough args.");
            System.exit(1);
        }
        // System.out.println("Scrpit Path: " + args[0]);

        for (int i = 1; i < args.length; i++) {
            realArgs.add(args[i]);
        }

        MapperArgs mapperArgs = new MapperArgs();
        try {
            new JCommander(mapperArgs, realArgs.toArray(new String[0]));
        } catch (Exception e) {
            JCommander.getConsole().println("Cannot parse arguments: " + e.getClass() + " -> " + e.getMessage());
            System.exit(1);
        }

        if (mapperArgs.daemonParemeters != null && !mapperArgs.daemonParemeters.isEmpty()) {
            DoDoDaemon d = new DoDoDaemon();
            d.start();
        } else {

            System.out.println("The stream - cluster pairs: ");
            System.out.println(mapperArgs.peClusterMap.toString());

            try {
                ZkClient zkClient = new ZkClient(mapperArgs.zkConnectionString, mapperArgs.timeout);
                mapperArgs.peClusterMap.applyToZooKeeper(zkClient);
            } catch (Exception e) {
                System.out.println("apply changes to ZooKeeper failed: " + e.getClass() + " -> " + e.getMessage());
            }
        }
    }

    public static class PEClusterMapperConverter implements IStringConverter<PEClusterMapper> {
        @Override
        public PEClusterMapper convert(String value) {
            PEClusterMapper result = new PEClusterMapper();
            String[] pieces = value.split(",");
            for (String piece : pieces) {
                String[] s = piece.split("=");
                if (s.length < 2) {
                    System.out.println("There is something wrong with the input params: " + value);
                    System.exit(1);
                }
                Map<String, StreamFlow> tmpMap = result.clusterMap.get(s[0]);
                if (tmpMap == null) {
                    tmpMap = Maps.newHashMap();
                    result.clusterMap.put(s[0], tmpMap);
                }
                tmpMap.put(s[1], new StreamFlow(s[1]));
            }
            return result;
        }
    }

    static class MapperArgs {
        @Parameter
        private List<String> parameters = new ArrayList<String>();

        @Parameter(names = "-zk", description = "ZooKeeper connection string")
        String zkConnectionString = "localhost:2181";

        @Parameter(names = "-timeout", description = "Connection timeout to Zookeeper, in ms")
        int timeout = 10000;

        @Parameter(names = "-map", description = "new mapping for stream and cluster", converter = PEClusterMapperConverter.class)
        PEClusterMapper peClusterMap = new PEClusterMapper();

        @Parameter(names = "-d", description = "start daemon")
        String daemonParemeters = null;
    }

    static class Stream {

        private final ZkClient zkClient;
        private final String consumerPath;
        private final String producerPath;

        String streamName;
        Set<String> producers = new HashSet<String>();// cluster name
        Set<String> consumers = new HashSet<String>();// cluster name

        Map<String, String> clusterAppMap = Maps.newHashMap();

        public Stream(String streamName, ZkClient zkClient) throws Exception {
            this.streamName = streamName;
            this.zkClient = zkClient;
            this.consumerPath = "/s4/streams/" + streamName + "/consumers";
            this.producerPath = "/s4/streams/" + streamName + "/producers";
            readStreamFromZk();
        }

        private void readStreamFromZk() throws Exception {
            List<String> consumerNodes = zkClient.getChildren(consumerPath);
            for (String node : consumerNodes) {
                ZNRecord consumer = zkClient.readData(consumerPath + "/" + node, true);
                consumers.add(consumer.getSimpleField("clusterName"));
            }

            List<String> producerNodes = zkClient.getChildren(producerPath);
            for (String node : producerNodes) {
                ZNRecord consumer = zkClient.readData(producerPath + "/" + node, true);
                producers.add(consumer.getSimpleField("clusterName"));
            }

            getAppNames();
        }

        private void getAppNames() {
            Set<String> clusters = new HashSet<String>(consumers);
            clusters.addAll(producers);
            for (String cluster : clusters) {
                clusterAppMap.put(cluster, getApp(cluster, zkClient));
            }
        }

        public boolean containsCluster(String cluster) {
            if (producers.contains(cluster) || consumers.contains(cluster)) {
                return true;
            }
            return false;
        }

        private static String getApp(String clusterName, ZkClient zkClient) {
            String appPath = "/s4/clusters/" + clusterName + "/app/s4App";
            if (zkClient.exists(appPath)) {
                AppConfig appConfig = new AppConfig((ZNRecord) zkClient.readData("/s4/clusters/" + clusterName
                        + "/app/s4App"));
                return appConfig.getAppName();
            }
            return NONE;
        }
    }

    static class App {
        private String name = NONE;
        private String cluster;
        private String uri = NONE;
    }

    static class Cluster {
        private final ZkClient zkClient;
        private final String taskPath;
        private final String processPath;
        private final String appPath;

        String clusterName;
        int taskNumber;
        App app;

        List<ClusterNode> nodes = new ArrayList<ClusterNode>();

        public Cluster(String clusterName, ZkClient zkClient) throws Exception {
            this.clusterName = clusterName;
            this.zkClient = zkClient;
            this.taskPath = "/s4/clusters/" + clusterName + "/tasks";
            this.processPath = "/s4/clusters/" + clusterName + "/process";
            this.appPath = "/s4/clusters/" + clusterName + "/app/s4App";
            readClusterFromZk();
        }

        public void readClusterFromZk() throws Exception {
            List<String> processes;
            List<String> tasks;

            tasks = zkClient.getChildren(taskPath);
            processes = zkClient.getChildren(processPath);

            taskNumber = tasks.size();

            for (int i = 0; i < processes.size(); i++) {
                ZNRecord process = zkClient.readData(processPath + "/" + processes.get(i), true);
                if (process != null) {
                    int partition = Integer.parseInt(process.getSimpleField("partition"));
                    String host = process.getSimpleField("host");
                    int port = Integer.parseInt(process.getSimpleField("port"));
                    String taskId = process.getSimpleField("taskId");
                    ClusterNode node = new ClusterNode(partition, port, host, taskId);
                    nodes.add(node);
                }
            }

            app = new App();
            app.cluster = clusterName;
            try {
                ZNRecord appRecord = zkClient.readData(appPath);
                AppConfig appConfig = new AppConfig(appRecord);
                app.name = appConfig.getAppName();
                app.uri = appConfig.getAppURI();
            } catch (ZkNoNodeException e) {
                System.out.println(appPath + " doesn't exist");
            }
        }
    }

}
