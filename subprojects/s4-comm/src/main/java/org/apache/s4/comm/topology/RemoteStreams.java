package org.apache.s4.comm.topology;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.I0Itec.zkclient.IZkChildListener;
import org.I0Itec.zkclient.IZkStateListener;
import org.I0Itec.zkclient.serialize.ZkSerializer;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.google.inject.name.Named;

/**
 * <p>
 * Monitors streams available in the S4 cluster.
 * </p>
 * <p>
 * Maintains a data structure reflecting the currently published streams with their consumers and publishers.
 * </p>
 * <p>
 * Provides methods to publish producers and consumers of streams
 * </p>
 * 
 */
@Singleton
public class RemoteStreams implements IZkStateListener, IZkChildListener {

    private static final Logger logger = LoggerFactory.getLogger(ClustersFromZK.class);
    private KeeperState state;
    private final ZkClient zkClient;
    private final Lock lock;
    private final static String STREAMS_PATH = "/s4/streams";
    // by stream name, then "producer"|"consumer" then
    private Map<String, Map<String, Set<StreamConsumer>>> streams = new HashMap<String, Map<String, Set<StreamConsumer>>>();

    public enum StreamType {
        PRODUCER, CONSUMER;

        public String getPath(String streamName) {
            switch (this) {
                case PRODUCER:
                    return STREAMS_PATH + "/" + streamName + "/" + getCollectionName();
                case CONSUMER:
                    return STREAMS_PATH + "/" + streamName + "/" + getCollectionName();
                default:
                    throw new RuntimeException("Invalid path in enum StreamType");
            }
        }

        public String getCollectionName() {
            switch (this) {
                case PRODUCER:
                    return "producers";
                case CONSUMER:
                    return "consumers";
                default:
                    throw new RuntimeException("Invalid path in enum StreamType");
            }
        }
    }

    @Inject
    public RemoteStreams(@Named("cluster.zk_address") String zookeeperAddress,
            @Named("cluster.zk_session_timeout") int sessionTimeout,
            @Named("cluster.zk_connection_timeout") int connectionTimeout) throws Exception {

        lock = new ReentrantLock();
        zkClient = new ZkClient(zookeeperAddress, sessionTimeout, connectionTimeout);
        ZkSerializer serializer = new ZNRecordSerializer();
        zkClient.setZkSerializer(serializer);
        zkClient.subscribeStateChanges(this);
        zkClient.waitUntilConnected(connectionTimeout, TimeUnit.MILLISECONDS);
        // bug in zkClient, it does not invoke handleNewSession the first time
        // it connects
        this.handleStateChanged(KeeperState.SyncConnected);

        this.handleNewSession();

    }

    public Set<StreamConsumer> getConsumers(String streamName) {
        if (!streams.containsKey(streamName)) {
            return Collections.emptySet();
        } else {
            return streams.get(streamName).get("consumers");
        }
    }

    /**
     * One method to do any processing if there is a change in ZK, all callbacks will be processed sequentially
     */
    private void doProcess() {
        lock.lock();
        try {
            refreshStreams();
        } catch (Exception e) {
            logger.warn("Exception in tryToAcquireTask", e);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void handleStateChanged(KeeperState state) throws Exception {
        this.state = state;
    }

    @Override
    public void handleNewSession() throws Exception {
        logger.info("New session:" + zkClient.getSessionId());
        zkClient.subscribeChildChanges(STREAMS_PATH, this);

        doProcess();
    }

    @Override
    public void handleChildChange(String paramString, List<String> paramList) throws Exception {
        doProcess();
    }

    private void refreshStreams() {
        List<String> children = zkClient.getChildren(STREAMS_PATH);
        for (String streamName : children) {
            if (!streams.containsKey(streamName)) {
                logger.info("Detected new stream [{}]", streamName);
                streams.put(streamName, new HashMap<String, Set<StreamConsumer>>());
                zkClient.subscribeChildChanges(StreamType.PRODUCER.getPath(streamName), this);
                zkClient.subscribeChildChanges(StreamType.CONSUMER.getPath(streamName), this);
                streams.put(streamName, new HashMap<String, Set<StreamConsumer>>());
            }

            update(streamName, StreamType.PRODUCER);
            update(streamName, StreamType.CONSUMER);
        }
    }

    private void update(String streamName, StreamType type) {
        List<String> elements = zkClient.getChildren(type.getPath(streamName));
        Set<StreamConsumer> consumers = new HashSet<StreamConsumer>();
        for (String element : elements) {
            ZNRecord producerData = zkClient.readData(type.getPath(streamName) + "/" + element, true);
            if (producerData != null) {
                StreamConsumer consumer = new StreamConsumer(Integer.valueOf(producerData.getSimpleField("appId")),
                        producerData.getSimpleField("clusterName"));
                consumers.add(consumer);
            }
        }
        streams.get(streamName).put(type.getCollectionName(), Collections.unmodifiableSet(consumers));
    }

    public void addOutputStream(String appId, String clusterName, String streamName) {
        lock.lock();
        try {
            logger.debug("Adding output stream [{}] for app [{}] in cluster [{}]", new String[] { streamName, appId,
                    clusterName });
            createStreamPaths(streamName);
            ZNRecord producer = new ZNRecord(streamName + "/" + clusterName + "/" + appId);
            producer.putSimpleField("appId", appId);
            producer.putSimpleField("clusterName", clusterName);
            try {
                zkClient.createEphemeralSequential(StreamType.PRODUCER.getPath(streamName) + "/producer-", producer);
            } catch (Throwable e) {
                logger.error("Exception trying to create producer stream [{}] for app [{}] and cluster [{}] : [{}] :",
                        new String[] { streamName, appId, clusterName, e.getMessage() });
            }
            refreshStreams();
        } finally {
            lock.unlock();
        }
    }

    /**
     * Creates (it they don't exist yet) persistent znodes for producers and consumers of a stream.
     */
    private void createStreamPaths(String streamName) {
        zkClient.createPersistent(StreamType.PRODUCER.getPath(streamName), true);
        zkClient.createPersistent(StreamType.CONSUMER.getPath(streamName), true);
    }

    /**
     * Publishes interest in a stream from an application.
     * 
     * @param appId
     * @param clusterName
     * @param streamName
     */
    public void addInputStream(int appId, String clusterName, String streamName) {
        lock.lock();
        try {
            logger.debug("Adding input stream [{}] for app [{}] in cluster [{}]",
                    new String[] { streamName, String.valueOf(appId), clusterName });
            createStreamPaths(streamName);
            ZNRecord consumer = new ZNRecord(streamName + "/" + clusterName + "/" + appId);
            consumer.putSimpleField("appId", String.valueOf(appId));
            consumer.putSimpleField("clusterName", clusterName);
            try {
                // NOTE: We create 1 sequential znode per consumer node instance
                zkClient.createEphemeralSequential(StreamType.CONSUMER.getPath(streamName) + "/consumer-", consumer);
            } catch (Throwable e) {
                logger.error("Exception trying to create consumer stream [{}] for app [{}] and cluster [{}] : [{}] :",
                        new String[] { streamName, String.valueOf(appId), clusterName, e.getMessage() });
            }
            refreshStreams();
        } finally {
            lock.unlock();
        }
    }
}