package ca.concordia.ece.sac.s4app.moviesreco;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.s4.base.Event;
import org.apache.s4.base.GenericKeyFinder;
import org.apache.s4.base.KeyFinder;
import org.apache.s4.core.App;
import org.apache.s4.core.RemoteStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.graphite.Graphite;
import com.codahale.metrics.graphite.GraphiteReporter;
import com.google.common.collect.ImmutableList;

public class RecoApp extends App {

    private static Logger logger = LoggerFactory.getLogger(RecoApp.class);
    static final private String recoStreamName = "MovieReco";
    static final private String mutualFansStreamName = "MutualFan";
    static final private String ratingStreamName = "RawRating";

    private RemoteStream recoStream;
    private RemoteStream mutualFansStream;

    @Override
    protected void onStart() {
        // TODO Auto-generated method stub

    }

    @Override
    protected void onInit() {
        try {
            prepare();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void prepare() throws Exception {
        // uncomment the following in order to get metrics outputs in .csv files
        // prepareMetricsOutputs();

        RecoOutputPE outputPE = createPE(RecoOutputPE.class);
        outputPE.setTimerInterval(10, TimeUnit.SECONDS);

        recoStream = createOutputStream(recoStreamName, new KeyFinder<Event>() {

            @Override
            public List<String> get(final Event arg0) {
                return ImmutableList.of("aggregationKey");
            }
        });

        createInputStream(recoStreamName, new KeyFinder<Event>() {

            @Override
            public List<String> get(final Event arg0) {
                return ImmutableList.of("aggregationKey");
            }
        }, outputPE);

        SimilarMoviesPE similarPE = createPE(SimilarMoviesPE.class);
        similarPE.setDownstream(recoStream);
        similarPE.setTimerInterval(10, TimeUnit.SECONDS);

        KeyFinder<Event> kf = new GenericKeyFinder<Event>("mId", Event.class);
        mutualFansStream = createOutputStream(mutualFansStreamName, kf);
        createInputStream(mutualFansStreamName, kf, similarPE);

        KeyFinder<Event> kfUser = new GenericKeyFinder<Event>("uId",
                Event.class);
        HistoryPE historyPE = createPE(HistoryPE.class);
        historyPE.setDownstream(mutualFansStream);
        createInputStream(ratingStreamName, kfUser, historyPE);
    }

    private void prepareMetricsOutputs() throws IOException {
        final Graphite graphite = new Graphite(new InetSocketAddress(
                "10.1.1.2", 2003));
        final GraphiteReporter reporter = GraphiteReporter
                .forRegistry(this.getMetricRegistry())
                .prefixedWith("S4-" + getClusterName() + "-" + getPartitionId())
                .convertRatesTo(TimeUnit.SECONDS)
                .convertDurationsTo(TimeUnit.MILLISECONDS)
                .filter(MetricFilter.ALL).build(graphite);
        reporter.start(6, TimeUnit.SECONDS);
    }

    @Override
    protected void onClose() {
        // TODO Auto-generated method stub

    }

}
