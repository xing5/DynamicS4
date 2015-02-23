package org.apache.s4.ddm;

import java.util.Timer;
import java.util.TimerTask;

import org.apache.s4.comm.topology.ZkClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DoDoDaemon {
    static Logger logger = LoggerFactory.getLogger(DoDoDaemon.class);
    final private Timer triggerTimer;
    HealthStats h;
    ZkClient zk = null;

    public DoDoDaemon(ZkClient zkClient, String paras) {
        triggerTimer = new Timer();
        zk = zkClient;
        h = new HealthStats();
    }

    private class OnTimeTask extends TimerTask {

        @Override
        public void run() {
            System.out.println("Hi!");
            try {
                h.initData();
                h.printData();
                h.orderClusters();
                h.printOrderedClusters();
                PEClusterMapper pm = h.analyze();
                if (pm != null) {
                    pm.applyToZooKeeper(zk);
                    Thread.sleep(10 * 60 * 1000);
                }
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
