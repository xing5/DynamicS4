package ca.concordia.ece.sac.s4app.moviesreco;

import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.s4.base.Event;
import org.apache.s4.core.App;
import org.apache.s4.core.ProcessingElement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Charsets;
import com.google.common.io.Files;

public class RecoOutputPE extends ProcessingElement {

    private static Logger logger = LoggerFactory.getLogger(RecoOutputPE.class);
    Map<String, String> outputMap;

    public RecoOutputPE() {
        // TODO Auto-generated constructor stub
    }

    public RecoOutputPE(App app) {
        super(app);
        // TODO Auto-generated constructor stub
    }

    public void onEvent(Event event) {
        // logger.debug("TopNTopicPE [" + getId() +
        // "] receive event, topic: [{}] count: [{}]", event.get("topic",
        // String.class), event.get("count", int.class));
        outputMap.put(event.get("mId", String.class),
                event.get("top", String.class));
    }

    public void onTime() {
        File f = new File("MovieReco.txt");

        StringBuilder sb = new StringBuilder();
        int i = 0;
        sb.append("----\n"
                + new SimpleDateFormat("yyyy-MMM-dd HH:mm:ss")
                        .format(new Date()) + "\n");

        for (Entry<String, String> entry : outputMap.entrySet()) {
            sb.append(entry.getKey() + "\t" + entry.getValue() + "\n");
        }
        sb.append("\n");
        try {
            Files.append(sb.toString(), f, Charsets.UTF_8);
            logger.info("Wrote Recommendations to file [{}] ",
                    f.getAbsolutePath());
        } catch (IOException e) {
            logger.error("Cannot write Recommendations to file [{}]",
                    f.getAbsolutePath(), e);
        }
    }

    @Override
    protected void onCreate() {
        // TODO Auto-generated method stub
        outputMap = new HashMap<String, String>();

    }

    @Override
    protected void onRemove() {
        // TODO Auto-generated method stub

    }

}
