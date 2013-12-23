package org.apache.s4.ddm;

import java.util.Timer;
import java.util.TimerTask;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DoDoDaemon {
    static Logger logger = LoggerFactory.getLogger(MapperTool.class);
    final private Timer triggerTimer;

    public DoDoDaemon() {
        triggerTimer = new Timer();
    }

    private class OnTimeTask extends TimerTask {

        @Override
        public void run() {
            System.out.println("hi");
        }
    }

    protected void start() {
        if (triggerTimer != null) {
            triggerTimer.scheduleAtFixedRate(new OnTimeTask(), 0, 2 * 1000);
        }

    }

    protected void stop() {
        if (triggerTimer != null) {
            triggerTimer.cancel();
        }
    }
}
