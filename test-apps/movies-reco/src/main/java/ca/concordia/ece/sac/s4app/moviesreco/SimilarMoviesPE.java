package ca.concordia.ece.sac.s4app.moviesreco;

import java.util.Iterator;
import java.util.Map;
import java.util.TreeSet;

import org.apache.s4.base.Event;
import org.apache.s4.core.App;
import org.apache.s4.core.ProcessingElement;
import org.apache.s4.core.RemoteStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

public class SimilarMoviesPE extends ProcessingElement {

    private static Logger logger = LoggerFactory
            .getLogger(SimilarMoviesPE.class);
    Map<String, Integer> similarMovies;
    String lastReport = "";
    boolean bGotEvents = false;
    transient RemoteStream downStream;

    public SimilarMoviesPE() {
        // TODO Auto-generated constructor stub
    }

    public SimilarMoviesPE(App app) {
        super(app);
        // TODO Auto-generated constructor stub
    }

    public void setDownstream(RemoteStream s) {
        this.downStream = s;
    }

    public RemoteStream getDownstream() {
        return downStream;
    }

    public void onEvent(Event event) {
        bGotEvents = true;
        String strMovieId = event.get("mId2", String.class);
        logger.trace(
                "movie [{}] received a mutual fan event from movie id [{}]",
                getId(), strMovieId);
        Integer count = similarMovies.get(strMovieId);
        if (count == null) {
            count = 1;
        } else {
            count++;
        }
        similarMovies.put(strMovieId, count);
        logger.trace("mutual [{},{}] count: " + count, getId(), strMovieId);
        logger.trace("similarmap of {}:\n" + similarMovies.toString(), getId());

    }

    @Override
    public void onTime() {
        if (!bGotEvents) {
            return;
        }
        bGotEvents = false;
        TreeSet<TopNEntry> sortedMovies = Sets.newTreeSet();
        for (Map.Entry<String, Integer> topMovie : similarMovies.entrySet()) {
            sortedMovies.add(new TopNEntry(topMovie.getKey(), topMovie
                    .getValue()));
        }

        String strTopList = "";
        int i = 0;
        Iterator<TopNEntry> iterator = sortedMovies.iterator();

        while (iterator.hasNext() && i < 30) {
            TopNEntry entry = iterator.next();
            if (i == 0) {
                strTopList = entry.movie;
            } else {
                strTopList += "-" + entry.movie;
            }
            i++;
        }

        if (!strTopList.equals(lastReport)) {
            Event e = new Event();
            e.put("mId", String.class, getId());
            e.put("top", String.class, strTopList);
            downStream.put(e);
            lastReport = strTopList;
        }
    }

    @Override
    protected void onCreate() {
        // TODO Auto-generated method stub
        similarMovies = Maps.newHashMap();

    }

    @Override
    protected void onRemove() {
        // TODO Auto-generated method stub
    }

    class TopNEntry implements Comparable<TopNEntry> {
        String movie = null;
        int count = 0;

        public TopNEntry(String movie, int count) {
            this.movie = movie;
            this.count = count;
        }

        @Override
        public int compareTo(TopNEntry topNEntry) {
            if (topNEntry.count < this.count) {
                return -1;
            } else {
                return 1;
            }
        }

        @Override
        public String toString() {
            return "movie:" + movie + " count:" + count;
        }
    }

}
