package ca.concordia.ece.sac.s4app.moviesreco;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.apache.s4.base.Event;
import org.apache.s4.core.App;
import org.apache.s4.core.ProcessingElement;
import org.apache.s4.core.RemoteStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HistoryPE extends ProcessingElement {

    private static Logger logger = LoggerFactory.getLogger(HistoryPE.class);
    List<String> favMovies;
    transient RemoteStream downStream;

    public HistoryPE() {
        super();
        // TODO Auto-generated constructor stub
    }

    public HistoryPE(App app) {
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
        String strMovieId = event.get("mId", String.class);
        logger.trace("userid [{}] movie id [{}]", getId(), strMovieId);
        List<Event> newEvents = new ArrayList<Event>();
        boolean bExist = false;
        for (String favMovie : favMovies) {
            if (favMovie.equals(strMovieId)) {
                logger.trace("This is an existing movie id.");
                bExist = true;
            } else {
                newEvents
                        .addAll(createEventsForMutualFan(strMovieId, favMovie));
            }
        }
        if (bExist) {
            return;
        }
        favMovies.add(strMovieId);
        for (Event e : newEvents) {
            downStream.put(e);
        }
    }

    private Collection<Event> createEventsForMutualFan(String strMovieId,
            String favMovie) {
        Event[] array = { new Event(), new Event() };
        array[0].put("mId", String.class, strMovieId);
        array[0].put("mId2", String.class, favMovie);
        array[1].put("mId", String.class, favMovie);
        array[1].put("mId2", String.class, strMovieId);
        return Arrays.asList(array);
    }

    @Override
    protected void onCreate() {
        // TODO Auto-generated method stub
        favMovies = new ArrayList<String>();

    }

    @Override
    protected void onRemove() {
        // TODO Auto-generated method stub

    }

}
