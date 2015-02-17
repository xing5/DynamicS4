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
import java.io.FileInputStream;
import java.io.IOException;
import java.lang.Thread.UncaughtExceptionHandler;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.util.Map;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.TimerTask;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import org.apache.s4.base.Event;
import org.apache.s4.base.util.S4MetricsRegistry;
import org.apache.s4.core.adapter.AdapterApp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.graphite.Graphite;
import com.codahale.metrics.graphite.GraphiteReporter;
import com.google.common.util.concurrent.RateLimiter;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import org.apache.commons.math3.distribution.ZipfDistribution;

import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;
import twitter4j.conf.ConfigurationBuilder;
import twitter4j.StallWarning;

public class TwitterInputAdapter extends AdapterApp {

    private static Logger logger = LoggerFactory.getLogger(TwitterInputAdapter.class);

    public TwitterInputAdapter() {
    }

    private LinkedBlockingQueue<String> messageQueue = new LinkedBlockingQueue<String>();

    protected ServerSocket serverSocket;

    private Thread t;
    private Thread srcStream;
    ZipfDistribution zd = new ZipfDistribution(5000, 0.5);
    List<Integer> sampleList = new ArrayList<Integer>();
    
    private int rateIncreasement;
    private String graphiteServerIP;
	private int sampleIndex = 0;

    @Override
    protected void onClose() {
    }

    @Override
    protected void onInit() {
        super.onInit();
        try {
            loadSettings();
            zd.reseedRandomGenerator(42);
            prepareMetricsOutputs(graphiteServerIP);
        } catch (Exception e) {
            logger.error("Cannot start metrics");
        }
        t = new Thread(new Dequeuer());
        srcStream = new Thread(new ProduceZipf());
    }
    
    private void loadSettings() throws Exception {
        File exprSettingFile = new File(System.getProperty("user.home") + "/expr.settings");
        if (!exprSettingFile.exists()) {
        	logger.error("Cannot find configuration file: ", exprSettingFile.getAbsolutePath());
        	rateIncreasement = 45;
        	graphiteServerIP = "10.0.1.10";
        } else {
        	Properties exprSettings = new Properties();
        	exprSettings.load(new FileInputStream(exprSettingFile));
        	graphiteServerIP = exprSettings.getProperty("graphite.server.ip");
        	rateIncreasement = Integer.parseInt(exprSettings.getProperty("src.rate.increasement"));
        }
    }
//    public void connectAndRead() throws Exception {
//
//        ConfigurationBuilder cb = new ConfigurationBuilder();
//        Properties twitterProperties = new Properties();
//        File twitter4jPropsFile = new File(System.getProperty("user.home") + "/twitter4j.properties");
//        if (!twitter4jPropsFile.exists()) {
//            logger.error(
//                    "Cannot find twitter4j.properties file in this location :[{}]. Make sure it is available at this place and includes oauth credentials",
//                    twitter4jPropsFile.getAbsolutePath());
//            return;
//        }
//        twitterProperties.load(new FileInputStream(twitter4jPropsFile));
//
//        cb.setDebugEnabled(false)
//                .setOAuthConsumerKey(twitterProperties.getProperty("oauth.consumerKey"))
//                .setOAuthConsumerSecret(twitterProperties.getProperty("oauth.consumerSecret"))
//                .setOAuthAccessToken(twitterProperties.getProperty("oauth.accessToken"))
//                .setOAuthAccessTokenSecret(twitterProperties.getProperty("oauth.accessTokenSecret"));
//        TwitterStream twitterStream = new TwitterStreamFactory(cb.build()).getInstance();
//        StatusListener statusListener = new StatusListener() {
//
//            @Override
//            public void onException(Exception ex) {
////                logger.error("error", ex);
//            }
//
//            @Override
//            public void onTrackLimitationNotice(int numberOfLimitedStatuses) {
////                logger.error("error");
//            }
//
//            @Override
//            public void onStatus(Status status) {
//                messageQueue.add(status);
//
//            }
//
//            @Override
//            public void onScrubGeo(long userId, long upToStatusId) {
////                logger.error("error");
//            }
//            
//            @Override
//            public void onStallWarning(StallWarning arg0) {
////                logger.error("error");
//            }
//            
//            @Override
//            public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice) {
////                logger.error("error");
//            }
//        };
//        twitterStream.addListener(statusListener);
//        twitterStream.sample();
//
//    }

    @Override
    protected void onStart() {
        try {
            t.start();
            srcStream.start();
            //connectAndRead();
            
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void prepareMetricsOutputs(String ip) throws IOException {
        final Graphite graphite = new Graphite(new InetSocketAddress(ip, 2003));
        final GraphiteReporter reporter = GraphiteReporter.forRegistry(this.getMetricRegistry()).prefixedWith("S4-" + getClusterName() + "-" + getPartitionId())
                .convertRatesTo(TimeUnit.SECONDS).convertDurationsTo(TimeUnit.MILLISECONDS)
                .filter(MetricFilter.ALL).build(graphite);
        reporter.start(6, TimeUnit.SECONDS);
    }
    
    private int getZipfSample(ZipfDistribution zd, List<Integer> sampleList) {
    	int rst;
    	if (sampleList.size() < 5000) {
    		rst = zd.sample();
    		sampleList.add(rst);
    	} else {
    		rst = sampleList.get(sampleIndex++);
    		sampleIndex %= sampleList.size();
    	}
    	return rst;
    }
    
    class ProduceZipf implements Runnable {
        
        private final Meter srcSuccMeter = getMetricRegistry().meter(MetricRegistry.name("event-src", "succ"));
        private final Meter srcFailMeter = getMetricRegistry().meter(MetricRegistry.name("event-src", "fail"));

        private final RateLimiter rateLimiter = RateLimiter.create(20000, 200, TimeUnit.SECONDS);
        
        @Override
        public void run() {
        	
            while (true) {
                try {
                	rateLimiter.acquire();
                    String tweet = "This is test #topic" + getZipfSample(zd, sampleList) + " generated by apache zipf distribution.";
                    messageQueue.add(tweet);
                    srcSuccMeter.mark();
                } catch (Exception e) {
                    srcFailMeter.mark();
                }
            }
        }
    }

    class Dequeuer implements Runnable {

        @Override
        public void run() {
            while (true) {
                try {
                    //logger.debug("try sending a event.");
                    //Status status = messageQueue.take();
                    String tweet = messageQueue.take();
                    //logger.debug("Sending event:" + tweet);
                    
                    Event event = new Event();
                    event.put("statusText", String.class, tweet);
                    getRemoteStream().put(event);
                } catch (Exception e) {

                }
            }

        }
    }
}
