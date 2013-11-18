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
package org.apache.s4.core.util;

import java.io.File;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.s4.base.Emitter;
import org.apache.s4.base.util.S4MetricsRegistry;
import org.apache.s4.comm.topology.Assignment;
import org.apache.s4.core.ProcessingElement;
import org.apache.s4.core.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.beust.jcommander.internal.Lists;
import com.beust.jcommander.internal.Maps;
import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.CsvReporter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.graphite.Graphite;
import com.codahale.metrics.graphite.GraphiteReporter;
import com.google.common.base.Strings;
import com.google.common.cache.LoadingCache;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.google.inject.name.Named;

/**
 * Utility class for centralizing system runtime metrics, such as information about event processing rates, cache
 * eviction etc...
 */
@Singleton
public class S4Metrics {

    private static Logger logger = LoggerFactory.getLogger(S4Metrics.class);

    static final Pattern METRICS_CONFIG_PATTERN = Pattern
            .compile("(csv:.+|console|graphite:.+):(\\d+):(DAYS|HOURS|MICROSECONDS|MILLISECONDS|MINUTES|NANOSECONDS|SECONDS)");

    @Inject
    Emitter emitter;

    @Inject
    Assignment assignment;

    @Inject(optional = true)
    @Named("s4.metrics.config")
    String metricsConfig;

    final MetricRegistry mr = S4MetricsRegistry.getMr();

    static List<Meter> partitionSenderMeters = Lists.newArrayList();

    private final Meter eventMeter = mr.meter(MetricRegistry.name("received", "event-count"));
    private final Meter bytesMeter = mr.meter(MetricRegistry.name("received", "bytes-count"));

    private final Meter localEventsMeter = mr.meter(MetricRegistry.name("sent", "sent-local"));
    private final Meter remoteEventsMeter = mr.meter(MetricRegistry.name("sent", "sent-remote"));

    private Meter[] senderMeters;

    private final Map<String, Meter> dequeuingStreamMeters = Maps.newHashMap();
    private final Map<String, Meter> droppedStreamMeters = Maps.newHashMap();
    private final Map<String, Meter> streamQueueFullMeters = Maps.newHashMap();
    private final Meter droppedInSenderMeter = mr.meter(MetricRegistry.name("dropped", "dropped@sender"));
    private final Meter droppedInRemoteSenderMeter = mr.meter(MetricRegistry.name("droppedr", "dropped@remote-sender"));

    private final Map<String, Meter[]> remoteSenderMeters = Maps.newHashMap();

    @Inject
    private void init() {

        if (Strings.isNullOrEmpty(metricsConfig)) {
            logger.info("Metrics reporting not configured");
        } else {
            Matcher matcher = METRICS_CONFIG_PATTERN.matcher(metricsConfig);
            if (!matcher.matches()) {
                logger.error(
                        "Invalid metrics configuration [{}]. Metrics configuration must match the pattern [{}]. Metrics reporting disabled.",
                        metricsConfig, METRICS_CONFIG_PATTERN);
            } else {
                String group1 = matcher.group(1);

                if (group1.startsWith("csv")) {
                    String outputDir = group1.substring("csv:".length());
                    long period = Long.valueOf(matcher.group(2));
                    TimeUnit timeUnit = TimeUnit.valueOf(matcher.group(3));
                    logger.info("Reporting metrics through csv files in directory [{}] with frequency of [{}] [{}]",
                            new String[] { outputDir, String.valueOf(period), timeUnit.name() });
                    CsvReporter reporter = CsvReporter.forRegistry(mr).formatFor(Locale.CANADA)
                            .convertRatesTo(TimeUnit.SECONDS).convertDurationsTo(TimeUnit.MILLISECONDS)
                            .build(new File(outputDir));
                    reporter.start(period, timeUnit);
                } else if (group1.startsWith("graphite")) {
                    final Graphite graphite = new Graphite(new InetSocketAddress("10.1.1.2", 2003));
                    final GraphiteReporter reporter = GraphiteReporter.forRegistry(mr).prefixedWith("S4")
                            .convertRatesTo(TimeUnit.SECONDS).convertDurationsTo(TimeUnit.MILLISECONDS)
                            .filter(MetricFilter.ALL).build(graphite);
                    reporter.start(1, TimeUnit.MINUTES);

                } else {
                    long period = Long.valueOf(matcher.group(2));
                    TimeUnit timeUnit = TimeUnit.valueOf(matcher.group(3));
                    logger.info("Reporting metrics on the console with frequency of [{}] [{}]",
                            new String[] { String.valueOf(period), timeUnit.name() });

                    ConsoleReporter reporter = ConsoleReporter.forRegistry(mr).convertRatesTo(TimeUnit.SECONDS)
                            .convertDurationsTo(TimeUnit.MILLISECONDS).build();
                    reporter.start(period, timeUnit);
                }
            }
        }

        senderMeters = new Meter[emitter.getPartitionCount()];
        // int localPartitionId = assignment.assignClusterNode().getPartition();
        for (int i = 0; i < senderMeters.length; i++) {
            senderMeters[i] = mr.meter(MetricRegistry.name("sender", "sent-to-" + (i)));
        }
        mr.register(MetricRegistry.name(Stream.class, "local-vs-remote"), new Gauge<Double>() {
            @Override
            public Double getValue() {
                // this will return NaN if divider is zero
                return localEventsMeter.getOneMinuteRate() / remoteEventsMeter.getOneMinuteRate();
            }
        });

    }

    public void createCacheGauges(ProcessingElement prototype, final LoadingCache<String, ProcessingElement> cache) {

        mr.register(MetricRegistry.name(prototype.getClass().getName() + "-cache-entries"), new Gauge<Long>() {

            @Override
            public Long getValue() {
                return cache.size();
            }
        });
        mr.register(MetricRegistry.name(prototype.getClass().getName() + "-cache-evictions"), new Gauge<Long>() {

            @Override
            public Long getValue() {
                return cache.stats().evictionCount();
            }
        });
        mr.register(MetricRegistry.name(prototype.getClass().getName() + "-cache-misses"), new Gauge<Long>() {

            @Override
            public Long getValue() {
                return cache.stats().missCount();
            }
        });
    }

    public void receivedEventFromCommLayer(int bytes) {
        eventMeter.mark();
        bytesMeter.mark(bytes);
    }

    public void queueIsFull(String name) {
        streamQueueFullMeters.get(name).mark();

    }

    public void sentEvent(int partition) {
        remoteEventsMeter.mark();
        try {
            senderMeters[partition].mark();
        } catch (NullPointerException e) {
            logger.warn("Sender meter not ready for partition {}", partition);
        } catch (ArrayIndexOutOfBoundsException e) {
            logger.warn("Partition {} does not exist", partition);
        }
    }

    public void droppedEventInSender() {
        droppedInSenderMeter.mark();
    }

    public void droppedEventInRemoteSender() {
        droppedInRemoteSenderMeter.mark();
    }

    public void sentLocal() {
        localEventsMeter.mark();
    }

    public void createStreamMeters(String name) {
        // TODO avoid maps to avoid map lookups?
        dequeuingStreamMeters.put(name, mr.meter(MetricRegistry.name("dequeued@" + name)));
        droppedStreamMeters.put(name, mr.meter(MetricRegistry.name("dropped@" + name)));
        streamQueueFullMeters.put(name, mr.meter(MetricRegistry.name("stream-full@" + name)));
    }

    public void dequeuedEvent(String name) {
        dequeuingStreamMeters.get(name).mark();
    }

    public void droppedEvent(String streamName) {
        droppedStreamMeters.get(streamName).mark();
    }

    public void createRemoteStreamMeters(String remoteClusterName, int partitionCount) {
        Meter[] meters = new Meter[partitionCount];
        for (int i = 0; i < partitionCount; i++) {
            meters[i] = mr.meter(MetricRegistry.name("remote-sender", remoteClusterName + "@partition-" + i));
        }
        synchronized (remoteSenderMeters) {
            remoteSenderMeters.put(remoteClusterName, meters);
        }

    }

    public void sentEventToRemoteCluster(String remoteClusterName, int partition) {
        remoteSenderMeters.get(remoteClusterName)[partition].mark();
    }

    public static class CheckpointingMetrics {

        static Meter rejectedSerializationTask = S4MetricsRegistry.getMr().meter(
                MetricRegistry.name("checkpointing", "checkpointing-rejected-serialization-task"));
        static Meter rejectedStorageTask = S4MetricsRegistry.getMr().meter(
                MetricRegistry.name("checkpointing", "checkpointing-rejected-storage-task"));
        static Meter fetchedCheckpoint = S4MetricsRegistry.getMr().meter(
                MetricRegistry.name("checkpointing", "checkpointing-fetched-checkpoint"));
        static Meter fetchedCheckpointFailure = S4MetricsRegistry.getMr().meter(
                MetricRegistry.name("checkpointing", "checkpointing-fetched-checkpoint-failed"));

        public static void rejectedSerializationTask() {
            rejectedSerializationTask.mark();
        }

        public static void rejectedStorageTask() {
            rejectedStorageTask.mark();
        }

        public static void fetchedCheckpoint() {
            fetchedCheckpoint.mark();
        }

        public static void checkpointFetchFailed() {
            fetchedCheckpointFailure.mark();
        }
    }

    public MetricRegistry getMetricRegistry() {
        // TODO Auto-generated method stub
        return mr;
    }

}
