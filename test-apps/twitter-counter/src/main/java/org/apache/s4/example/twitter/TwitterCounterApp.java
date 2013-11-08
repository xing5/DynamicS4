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

package org.apache.s4.example.twitter;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.commons.io.FileUtils;
import org.apache.s4.base.GenericKeyFinder;
import org.apache.s4.base.KeyFinder;
import org.apache.s4.base.Event;
import org.apache.s4.core.App;
import org.apache.s4.core.Stream;
import org.apache.s4.base.Event;
import org.apache.s4.core.RemoteStream;
import org.apache.s4.core.ft.CheckpointingConfig;
import org.apache.s4.core.ft.CheckpointingConfig.CheckpointingMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableList;
import com.yammer.metrics.reporting.CsvReporter;

public class TwitterCounterApp extends App {

    private static Logger logger = LoggerFactory.getLogger(TwitterCounterApp.class);
    static final private String aggregatedTopicStreamName = "AggregatedTopicSeen";
    static final private String topicSeenStreamName = "TopicSeen";
    static final private String rawInputStreamName = "RawStatus";
    private RemoteStream aggregatedTopicStream;
    private RemoteStream topicSeenStream;

    @Override
    protected void onClose() {
    }

    @Override
    protected void onInit() {
        try {
            prepare();
            startStream();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
    
    private void startStream() throws Exception{
        // Read the DAG distribution and start the stream
        if (getClusterName().equals("cluster1")) {
            logger.debug("Cluster1 starts receiving RawStatus and TopicSeen.");
            activateInputStream(rawInputStreamName);
            activateInputStream(topicSeenStreamName);
        }
        else if (getClusterName().equals("cluster3")) {
            logger.debug("Cluster3 starts receiving AggregatedTopic");
            activateInputStream(aggregatedTopicStreamName);
        }
    }
    
    private void prepare() throws Exception{
            // uncomment the following in order to get metrics outputs in .csv files
            prepareMetricsOutputs();

            TopNTopicPE topNTopicPE = createPE(TopNTopicPE.class);
            topNTopicPE.setTimerInterval(10, TimeUnit.SECONDS);
            // we checkpoint this PE every 20s
            topNTopicPE.setCheckpointingConfig(new CheckpointingConfig.Builder(CheckpointingMode.TIME).frequency(20)
                    .timeUnit(TimeUnit.SECONDS).build());
            
            aggregatedTopicStream = createOutputStream(aggregatedTopicStreamName, new KeyFinder<Event>() {

                @Override
                public List<String> get(final Event arg0) {
                    return ImmutableList.of("aggregationKey");
                }
            });
            
            prepareInputStream(aggregatedTopicStreamName, new KeyFinder<Event>() {

                @Override
                public List<String> get(final Event arg0) {
                    return ImmutableList.of("aggregationKey");
                }
            }, topNTopicPE);

            TopicCountAndReportPE topicCountAndReportPE = createPE(TopicCountAndReportPE.class);
            topicCountAndReportPE.setDownstream(aggregatedTopicStream);     
            topicCountAndReportPE.setTimerInterval(10, TimeUnit.SECONDS);
            // we checkpoint instances every 2 events
            topicCountAndReportPE.setCheckpointingConfig(new CheckpointingConfig.Builder(CheckpointingMode.EVENT_COUNT)
                    .frequency(2).build());

            
            KeyFinder<Event> kf = new GenericKeyFinder<Event>("topic", Event.class);
            topicSeenStream = createOutputStream(topicSeenStreamName, kf);
            prepareInputStream(topicSeenStreamName, kf, topicCountAndReportPE);

            TopicExtractorPE topicExtractorPE = createPE(TopicExtractorPE.class);
            topicExtractorPE.setDownStream(topicSeenStream);
            topicExtractorPE.setSingleton(true);
            prepareInputStream(rawInputStreamName, topicExtractorPE);
    }

    private void prepareMetricsOutputs() throws IOException {
        File metricsDirForPartition = new File("metrics/" + getClusterName() + "/" + getPartitionId());
        if (metricsDirForPartition.exists()) {
            FileUtils.deleteDirectory(metricsDirForPartition);
        }
        // activate metrics csv dump
        if (!metricsDirForPartition.mkdirs()) {
            LoggerFactory.getLogger(getClass()).error("Cannot create directory {}",
                    new File("metrics").getAbsolutePath());
        }
        CsvReporter.enable(metricsDirForPartition, 10, TimeUnit.SECONDS);
    }

    @Override
    protected void onStart() {

    }
}
