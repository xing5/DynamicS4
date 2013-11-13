/**
 * Map PE to clusters. 
 */
package org.apache.s4.ddm;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.s4.comm.topology.ZNRecord;
import org.apache.s4.comm.topology.ZNRecordSerializer;
import org.apache.s4.comm.topology.ZkClient;
import org.apache.s4.comm.topology.ZkRemoteStreams.StreamType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;

/**
 * @author xingwu
 * 
 */
public class PEClusterMapper {

    private static final Logger logger = LoggerFactory.getLogger(PEClusterMapper.class);
    private final Lock lock;

    public Map<String, Map<String, StreamFlow>> clusterMap;

    public PEClusterMapper() {
        lock = new ReentrantLock();
        clusterMap = Maps.newHashMap();
    }

    @Override
    public String toString() {
        String tmp = "";
        for (Map.Entry<String, Map<String, StreamFlow>> entry : clusterMap.entrySet()) {
            String streamName = entry.getKey();
            Map<String, StreamFlow> flows = entry.getValue();
            tmp += streamName + "\n";
            for (StreamFlow flow : flows.values()) {
                tmp += "    - " + flow.consumerClusterName + "\n";
            }
        }
        return tmp;
    }

    public void applyToZooKeeper(ZkClient zkClient) throws Exception {
        lock.lock();
        try {
            zkClient.setZkSerializer(new ZNRecordSerializer());
            StreamType type = StreamType.CONSUMER;

            // for each stream, check the consumer in ZooKeeper. If it's not the same, remove and recreate consumers
            for (Map.Entry<String, Map<String, StreamFlow>> entry : clusterMap.entrySet()) {
                String streamName = entry.getKey();
                Map<String, StreamFlow> flows = entry.getValue();
                Set<String> checkedClusters = new HashSet<String>();

                List<String> elements = zkClient.getChildren(type.getPath(streamName));

                // delete those not exist in flow
                for (String element : elements) {
                    ZNRecord consumerData = zkClient.readData(type.getPath(streamName) + "/" + element, true);
                    logger.debug("element {} consumerData: {}", element, consumerData);
                    if (consumerData != null) {
                        String clusterName = consumerData.getSimpleField("clusterName");
                        if (!flows.containsKey(clusterName)) {
                            logger.debug("REMAPPING: delete {} consumer: {}", streamName, clusterName);
                            zkClient.deleteRecursive(type.getPath(streamName) + "/" + element);

                            // add pe transmission information
                            if (checkedClusters.contains(clusterName)) {
                                continue;
                            }
                            checkedClusters.add(clusterName);
                            zkClient.createPersistent(StreamType.TRANSMISSION.getPath(streamName), true);
                            for (String consumerClusterName : flows.keySet()) {
                                ZNRecord peTrans = new ZNRecord(streamName + "/" + clusterName);
                                peTrans.putSimpleField("DestCluster", consumerClusterName);
                                try {
                                    zkClient.createPersistent(StreamType.TRANSMISSION.getPath(streamName) + "/"
                                            + clusterName, peTrans);
                                } catch (Throwable e) {
                                    logger.error(
                                            "Exception trying to create transmission stream [{}] for app [{}] and cluster [{}] : [{}] :",
                                            new String[] { streamName, clusterName, e.getMessage() });
                                }
                            }

                        } else {
                            flows.get(clusterName).exist = true;
                        }
                    }
                }

                // create consumers
                zkClient.createPersistent(StreamType.CONSUMER.getPath(streamName), true);

                for (StreamFlow flow : flows.values()) {
                    if (!flow.exist) {
                        logger.debug("REMAPPING: add {} consumer: {}", streamName, flow.consumerClusterName);
                        ZNRecord consumer = new ZNRecord(streamName + "/" + flow.consumerClusterName);
                        consumer.putSimpleField("clusterName", flow.consumerClusterName);
                        try {
                            // NOTE: We create 1 sequential znode per consumer node instance
                            zkClient.createPersistentSequential(StreamType.CONSUMER.getPath(streamName) + "/consumer-",
                                    consumer);
                        } catch (Throwable e) {
                            logger.error(
                                    "Exception trying to create consumer stream [{}] for app [{}] and cluster [{}] : [{}] :",
                                    new String[] { streamName, flow.consumerClusterName, e.getMessage() });
                        }
                    }
                }
            }
        } finally {
            lock.unlock();
        }
    }
}
