package ca.concordia.ece.sac.s4app.moviesreco;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Properties;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.s4.base.Event;
import org.apache.s4.base.GenericKeyFinder;
import org.apache.s4.base.KeyFinder;
import org.apache.s4.core.adapter.AdapterApp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.graphite.Graphite;
import com.codahale.metrics.graphite.GraphiteReporter;

public class RatingStreamApp extends AdapterApp {

    private static Logger logger = LoggerFactory
            .getLogger(RatingStreamApp.class);

    public RatingStreamApp() {
    }

    private final LinkedBlockingQueue<String> messageQueue = new LinkedBlockingQueue<String>();

    private Thread t;
    private Thread srcStream;
    private final Properties settings = new Properties();

    @Override
    protected void onClose() {
    }

    @Override
    protected void onInit() {
        super.onInit();
        try {
            readConfig();
            KeyFinder<Event> kfUser = new GenericKeyFinder<Event>("uId",
                    Event.class);
            setKeyFinder(kfUser);
            prepareMetricsOutputs();
        } catch (Exception e) {
            logger.error("Cannot start metrics");
        }
        t = new Thread(new Dequeuer());
        srcStream = new Thread(new ProduceRatingStream());
    }

    private void readConfig() throws Exception {
        File configFile = new File(System.getProperty("user.home")
                + "/moviereco.properties");
        if (!configFile.exists()) {
            logger.error(
                    "Cannot find moviereco.properties file in this location :[{}]. Make sure it is available at this place and includes oauth credentials",
                    configFile.getAbsolutePath());
            return;
        }
        settings.load(new FileInputStream(configFile));
        logger.trace("Read moviereco.properties success. a={}",
                settings.getProperty("moviereco.filepath.srcrating"));
        // settings.getProperty("oauth.consumerKey")
    }

    @Override
    protected void onStart() {
        try {
            t.start();
            srcStream.start();

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void prepareMetricsOutputs() throws IOException {
        final Graphite graphite = new Graphite(new InetSocketAddress(
                settings.getProperty("metrics.master"), 2003));
        final GraphiteReporter reporter = GraphiteReporter
                .forRegistry(this.getMetricRegistry())
                .prefixedWith("S4-" + getClusterName() + "-" + getPartitionId())
                .convertRatesTo(TimeUnit.SECONDS)
                .convertDurationsTo(TimeUnit.MILLISECONDS)
                .filter(MetricFilter.ALL).build(graphite);
        reporter.start(
                Integer.parseInt(settings.getProperty("metrics.interval")),
                TimeUnit.SECONDS);
    }

    class ProduceRatingStream implements Runnable {

        private final Meter srcSuccMeter = getMetricRegistry().meter(
                MetricRegistry.name("event-src", "succ"));
        private final Meter srcFailMeter = getMetricRegistry().meter(
                MetricRegistry.name("event-src", "fail"));

        @Override
        public void run() {
            try {
                while (true) {
                    String filename = settings
                            .getProperty("moviereco.filepath.srcrating");
                    logger.trace("read file {}", filename);
                    BufferedReader br = new BufferedReader(new FileReader(
                            filename));
                    String line = br.readLine();

                    while (line != null) {
                        messageQueue.add(line);
                        srcSuccMeter.mark();
                        line = br.readLine();
                        int interval = Integer.parseInt(settings
                                .getProperty("moviereco.interval"));
                        if (interval > 0) {
                            Thread.sleep(interval);
                        }
                    }
                    br.close();
                }
            } catch (Exception e) {
                logger.error("read file error");
            }
        }
    }

    class Dequeuer implements Runnable {

        @Override
        public void run() {
            while (true) {
                try {
                    String line = messageQueue.take();
                    String[] array = line.trim().split("\\s+");
                    if (array.length < 3 || Integer.parseInt(array[2]) < 3) {
                        continue;
                    }

                    Event event = new Event();
                    event.put("uId", String.class, array[0]);
                    event.put("mId", String.class, array[1]);
                    getRemoteStream().put(event);
                } catch (Exception e) {

                }
            }

        }
    }
}
