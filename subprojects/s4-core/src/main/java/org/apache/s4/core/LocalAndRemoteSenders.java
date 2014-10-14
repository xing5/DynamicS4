/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.s4.core;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;

import org.apache.s4.base.Event;
import org.apache.s4.base.Hasher;
import org.apache.s4.base.RemoteEmitter;
import org.apache.s4.base.SerializerDeserializer;
import org.apache.s4.comm.serialize.SerializerDeserializerFactory;
import org.apache.s4.comm.tcp.RemoteEmitters;
import org.apache.s4.comm.topology.Clusters;
import org.apache.s4.comm.topology.RemoteStreams;
import org.apache.s4.comm.topology.StreamConsumer;
import org.apache.s4.core.staging.RemoteSendersExecutorServiceFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.beust.jcommander.internal.Maps;
import com.google.inject.Inject;
import com.google.inject.Singleton;

/**
 * Default {@link RemoteSenders} implementation for sending events to nodes of a remote cluster.
 */
@Singleton
public class LocalAndRemoteSenders implements RemoteSenders {

    Logger logger = LoggerFactory.getLogger(LocalAndRemoteSenders.class);

    final RemoteEmitters remoteEmitters;

    final RemoteStreams remoteStreams;

    final Clusters remoteClusters;

    final SerializerDeserializer serDeser;

    final Hasher hasher;

    private App app = null;

    ConcurrentMap<String, RemoteSender> sendersByTopology = new ConcurrentHashMap<String, RemoteSender>();

    // combine a batch of pe instances into an special event with an id starting with "TRANS@"
    // streamName, partition, peIndex, event
    ConcurrentMap<String, Map<Integer, Map<Integer, Event>>> peEventCache = new ConcurrentHashMap<String, Map<Integer, Map<Integer, Event>>>();

    private final ExecutorService executorService;

    @Inject
    public LocalAndRemoteSenders(RemoteEmitters remoteEmitters, RemoteStreams remoteStreams, Clusters remoteClusters,
            SerializerDeserializerFactory serDeserFactory, Hasher hasher,
            RemoteSendersExecutorServiceFactory senderExecutorFactory) {
        this.remoteEmitters = remoteEmitters;
        this.remoteStreams = remoteStreams;
        this.remoteClusters = remoteClusters;
        this.hasher = hasher;
        executorService = senderExecutorFactory.create();

        serDeser = serDeserFactory.createSerializerDeserializer(Thread.currentThread().getContextClassLoader());
    }

    public void setApp(App app) {
        if (this.app == null) {
            this.app = app;
        }
    }

    public App getApp() {
        return this.app;
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.apache.s4.core.RemoteSenders#send(java.lang.String, org.apache.s4.base.Event)
     */
    @Override
    public void send(String hashKey, Event event) {

        Set<StreamConsumer> consumers = remoteStreams.getConsumers(event.getStreamId());
        for (StreamConsumer consumer : consumers) {
            // if destination is local cluster, send event to local stream
            // logger.debug("consumer cluster [{}] app cluster [{}]", consumer.getClusterName(),
            // getApp().getClusterName());
            if (consumer.getClusterName().equals(getApp().getClusterName())) {
                Streamable<Event> localStream = getApp().getStream(event.getStreamId());
                if (localStream != null) {
                    localStream.put(event);
                    continue;
                }
            }
            // NOTE: even though there might be several ephemeral znodes for the same app and topology, they are
            // represented by a single stream consumer
            RemoteSender sender = sendersByTopology.get(consumer.getClusterName());
            if (sender == null) {
                logger.debug("Creat RemoteSender. CLuster=[{}](" + remoteClusters.getCluster(consumer.getClusterName())
                        + ") [{}]", consumer.getClusterName(), remoteClusters.getCluster(consumer.getClusterName())
                        .getPhysicalCluster().getNodes());
                RemoteSender newSender = new RemoteSender(remoteEmitters.getEmitter(remoteClusters.getCluster(consumer
                        .getClusterName())), hasher, consumer.getClusterName());
                // TODO cleanup when remote topologies die
                sender = sendersByTopology.putIfAbsent(consumer.getClusterName(), newSender);
                if (sender == null) {
                    sender = newSender;
                }
            }
            // NOTE: this implies multiple serializations, there might be an optimization
            executorService.execute(new SendToRemoteClusterTask(hashKey, event, sender));
        }
    }

    /*
     * send PE to cluster.
     * 
     * @see org.apache.s4.core.RemoteSenders#sendPE(java.lang.String, byte[], java.lang.String, java.lang.String)
     */
    @Override
    public void sendPE(String key, byte[] peState, String streamName, int peIndex, String destClusterName) {
        Map<Integer, Map<Integer, Event>> eventCache = peEventCache.get(streamName);
        if (eventCache == null) {
            eventCache = Maps.newHashMap();
            logger.debug("new eventCache :" + streamName);
            peEventCache.put(streamName, eventCache);
        }

        RemoteEmitter rmEmitter = remoteEmitters.getEmitter(remoteClusters.getCluster(destClusterName));
        //int partition = (int) (hasher.hash(key) % rmEmitter.getPartitionCount());
        int partition = rmEmitter.getPartitionByConsistentHashing(key);
        Map<Integer, Event> peIndexEvent = eventCache.get(partition);
        if (peIndexEvent == null) {
            peIndexEvent = Maps.newHashMap();
            logger.debug("new peIndexEvent :" + partition);
            eventCache.put(partition, peIndexEvent);
        }

        Event event = peIndexEvent.get(peIndex);
        if (event == null) {
            event = new Event();
            event.setStreamId("TRANS@" + peIndex + "@" + streamName);
            logger.debug("new event : TRANS@" + peIndex + "@" + streamName);
            peIndexEvent.put(peIndex, event);
        }

        event.put(key, String.class, new String(peState));

        if (event.getDataSize() >= 100) {
            eventCache.remove(partition);
            logger.debug("event [{}] has enough data, preprare to send out", event.getStreamId());
            RemoteSender sender = sendersByTopology.get(destClusterName);
            if (sender == null) {
                logger.debug("Creat RemoteSender for PE transmission. CLuster=[{}]", destClusterName);
                RemoteSender newSender = new RemoteSender(rmEmitter, hasher, destClusterName);
                sender = sendersByTopology.putIfAbsent(destClusterName, newSender);
                if (sender == null) {
                    sender = newSender;
                }
            }
            executorService.execute(new SendToRemoteClusterTask(key, event, sender));
        }

    }

    /*
     * Send all cached pe state to remote cluster.
     * 
     * @see org.apache.s4.core.RemoteSenders#sendAllCachedPE(java.lang.String, java.lang.String)
     */
    @Override
    public void sendAllCachedPE(String streamName, String destClusterName) {
        Map<Integer, Map<Integer, Event>> eventCache = peEventCache.get(streamName);
        if (eventCache == null) {
            logger.debug("no stream " + streamName + " in eventCache");
            return;
        }

        for (Map<Integer, Event> peIndexEvent : eventCache.values()) {
            logger.debug("send all cached pe of stream [{}]", streamName);
            RemoteSender sender = sendersByTopology.get(destClusterName);
            if (sender == null) {
                logger.debug("Creat RemoteSender for PE transmission. CLuster=[{}]", destClusterName);
                RemoteSender newSender = new RemoteSender(remoteEmitters.getEmitter(remoteClusters
                        .getCluster(destClusterName)), hasher, destClusterName);
                sender = sendersByTopology.putIfAbsent(destClusterName, newSender);
                if (sender == null) {
                    sender = newSender;
                }
            }
            for (Event event : peIndexEvent.values()) {
                // all the key go to the same dest with same hash code
                for (String key : event.getAttributesAsMap().keySet()) {
                    executorService.execute(new SendToRemoteClusterTask(key, event, sender));
                    break;
                }
            }
        }
        eventCache.clear();
    }

    class SendToRemoteClusterTask implements Runnable {

        String hashKey;
        Event event;
        RemoteSender sender;

        public SendToRemoteClusterTask(String hashKey, Event event, RemoteSender sender) {
            super();
            this.hashKey = hashKey;
            this.event = event;
            this.sender = sender;
        }

        @Override
        public void run() {
            try {
                sender.send(hashKey, serDeser.serialize(event));
            } catch (InterruptedException e) {
                logger.error("Interrupted blocking send operation for event {}. Event is lost.", event);
                Thread.currentThread().interrupt();
            }

        }

    }
}
