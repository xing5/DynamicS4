package org.apache.s4.ddm;

import java.util.Timer;
import java.util.TimerTask;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DoDoDaemon {
    static Logger logger = LoggerFactory.getLogger(DoDoDaemon.class);
    final private Timer triggerTimer;

    public DoDoDaemon() {
        triggerTimer = new Timer();
    }

    private class OnTimeTask extends TimerTask {

        @Override
        public void run() {
            System.out.println("Hi!");
            HealthStats h = new HealthStats();
            try {
                h.initData();
                h.printData();
                h.orderClusters();
                h.printOrderedClusters();
                h.analyze();
            } catch (Exception e) {
                System.out.println("error:" + e.getMessage());
            }
        }
    }

    protected void start() {

        if (triggerTimer != null) {
            triggerTimer.scheduleAtFixedRate(new OnTimeTask(), 0, 60 * 1000);
        }

    }

    protected void stop() {
        if (triggerTimer != null) {
            triggerTimer.cancel();
        }
    }
}
