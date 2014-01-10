package org.apache.s4.ddm;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.net.URLConnection;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.beust.jcommander.internal.Maps;

public class HealthStats {
    static Logger logger = LoggerFactory.getLogger(HealthStats.class);

    private final static String statsUrl = "http://node-1.lan20.idsp.emulab.net:8080/render/?target=S4-cluster*.*.*-pe-processing-time.mean&target=S4-cluster*.*.*-pe-processing-time.m1_rate&format=csv&from=-5minutes";

    public class PeLoadStat {
        PeLoadStat() {
            eventsCount = 0;
            procTime = 0.0;
            eventsRound = 0;
            procRound = 0;
        }

        public double getLoad() {
            // logger.debug("{} {} load: " + eventsCount * procTime, eventsCount, procTime);
            return eventsCount * procTime;
        }

        double eventsCount;
        double procTime;
        int eventsRound;
        int procRound;
    }

    public class ClusterStats implements Comparable<ClusterStats> {
        public String name;
        public Double load;

        public ClusterStats(String n, double l) {
            name = n;
            load = l;
        }

        @Override
        public int compareTo(ClusterStats arg0) {
            return this.load > arg0.load ? 1 : this.load < arg0.load ? -1 : 0;
        }
    }

    // map<cluster, map<stream, s>>
    private Map<String, Map<String, List<PeLoadStat>>> mapStats = Maps.newHashMap();
    List<ClusterStats> orderedClusters = new ArrayList<ClusterStats>();
    private double CORRELATION_THRESHOLD = 0.0;
    private double DIFFERENCE_THRESHOLD = 1000;
    private double CAPACITY_THRESHOLD = 3000;

    public HealthStats() {
    }

    public HealthStats(double... para) {
        if (para.length > 0) {
            CORRELATION_THRESHOLD = para[0];
        }
        if (para.length > 1) {
            DIFFERENCE_THRESHOLD = para[1];
        }
    }

    private double getStreamNodesNum(String clusterName, String streamName) {
        Map<String, List<PeLoadStat>> streams = mapStats.get(clusterName);
        if (streams == null) {
            logger.debug("{} doesn't exists!", clusterName);
            return 0.0;
        }

        List<PeLoadStat> stats = streams.get(streamName);
        if (stats == null) {
            logger.debug("{}-{} doesn't exists!", clusterName, streamName);
            return 0.0;
        }
        return stats.get(0).eventsCount;
    }

    private double getClusterNodesNum(String clusterName) {
        Map<String, List<PeLoadStat>> streams = mapStats.get(clusterName);
        if (streams == null) {
            logger.debug("{} doesn't exists!", clusterName);
            return 0.0;
        }
        double num = 0.0;
        for (List<PeLoadStat> stats : streams.values()) {
            double tmpNum = stats.get(0).eventsCount;
            if (tmpNum > num) {
                num = tmpNum;
            }
        }
        return num;
    }

    private List<Double> getLoadSeriesOfStream(String clusterName, String streamName) {
        Map<String, List<PeLoadStat>> streams = mapStats.get(clusterName);
        if (streams == null) {
            logger.debug("{} doesn't exists!", clusterName);
            return null;
        }

        List<PeLoadStat> stats = streams.get(streamName);
        if (stats == null) {
            logger.debug("{}-{} doesn't exists!", clusterName, streamName);
            return null;
        }

        List<Double> list = new ArrayList<Double>();
        for (int i = 0; i < stats.size(); i++) {
            list.add(new Double(stats.get(i).getLoad()));
        }
        return list;
    }

    private List<Double> getLoadSeriesOfClusterExceptStream(String clusterName, String streamName) {
        Map<String, List<PeLoadStat>> streams = mapStats.get(clusterName);
        if (streams == null) {
            logger.debug("{} doesn't exists!", clusterName);
            return null;
        } else {
            List<Double> list = new ArrayList<Double>();
            for (Map.Entry<String, List<PeLoadStat>> streamEntrys : streams.entrySet()) {
                if (streamEntrys.getKey().equals(streamName))
                    continue;
                for (int i = 0; i < streamEntrys.getValue().size(); i++) {
                    Double d;
                    if (i < list.size()) {
                        d = list.get(i);
                    } else {
                        d = new Double(0.0);
                        list.add(d);
                    }
                    d += streamEntrys.getValue().get(i).getLoad();
                    list.set(i, d);
                }
            }
            return list;
        }
    }

    private List<Double> getLoadSeriesOfCluster(String clusterName) {
        Map<String, List<PeLoadStat>> streams = mapStats.get(clusterName);
        if (streams == null) {
            logger.debug("{} doesn't exists!", clusterName);
            return null;
        } else {
            List<Double> list = new ArrayList<Double>();
            for (List<PeLoadStat> streamStats : streams.values()) {
                for (int i = 0; i < streamStats.size(); i++) {
                    Double d;
                    if (i < list.size()) {
                        d = list.get(i);
                    } else {
                        d = new Double(0.0);
                        list.add(d);
                    }
                    d += streamStats.get(i).getLoad();
                    list.set(i, d);
                }
            }
            return list;
        }
    }

    public double averageLoad(List<Double> list) {
        if (list == null) {
            logger.debug("list is null");
            return 0.0;
        }
        double a = 0.0;
        for (Double d : list) {
            // logger.debug("v: " + d);
            a += d;
        }
        if (list.size() == 0) {
            logger.debug("list is empty");
            return 0.0;
        } else {
            return a / list.size();
        }
    }

    public double var(List<Double> list) {
        if (list.size() == 0)
            return 0;
        double sums2 = 0.0;
        double sums = 0.0;
        for (Double d : list) {
            sums2 += Math.pow(d, 2.0);
            sums += d;
        }
        return sums2 / list.size() - Math.pow(sums / list.size(), 2.0);
    }

    public double cov(List<Double> list1, List<Double> list2) {
        if (list1.size() == 0 || list2.size() == 0)
            return 0.0;
        double sums1s2 = 0.0;
        for (int i = 0; i < list1.size() && i < list2.size(); i++) {
            sums1s2 += list1.get(i) * list2.get(i);
        }
        return sums1s2 / list1.size() - averageLoad(list1) * averageLoad(list2);
    }

    public double correlation(List<Double> list1, List<Double> list2) {
        return cov(list1, list2) / (Math.sqrt(var(list1)) * Math.sqrt(var(list2)));
    }

    public void orderClusters() {
        for (String clusterName : mapStats.keySet()) {
            orderedClusters.add(new ClusterStats(clusterName, averageLoad(getLoadSeriesOfCluster(clusterName))));
        }
        Collections.sort(orderedClusters);
    }

    public void checkClusterPair(PEClusterMapper pm, String cluster1, String cluster2) {
        List<Double> list1 = getLoadSeriesOfCluster(cluster1);
        List<Double> list2 = getLoadSeriesOfCluster(cluster2);
        double corr = correlation(list1, list2);
        logger.debug("Correlation of {} and {} is " + corr, cluster1, cluster2);

        if (corr < CORRELATION_THRESHOLD && averageLoad(list1) > averageLoad(list2) + DIFFERENCE_THRESHOLD) {
            // find a PE to transmit
            List<ClusterStats> orderedStreams = new ArrayList<ClusterStats>();
            for (String streamName : mapStats.get(cluster1).keySet()) {
                logger.debug("compute " + streamName);
                double corrOfStream1 = correlation(getLoadSeriesOfStream(cluster1, streamName),
                        getLoadSeriesOfClusterExceptStream(cluster1, streamName));
                double corrOfStream2 = correlation(getLoadSeriesOfStream(cluster1, streamName),
                        getLoadSeriesOfCluster(cluster2));
                orderedStreams.add(new ClusterStats(streamName, (corrOfStream1 - corrOfStream2) / 2));
            }
            Collections.sort(orderedStreams);
            String streamToBeMoved = orderedStreams.get(orderedStreams.size() - 1).name;
            // check the cluster utilization to avoid infinite PE transmission
            double destLoad = averageLoad(getLoadSeriesOfStream(cluster1, streamToBeMoved))
                    * getStreamNodesNum(cluster1, streamToBeMoved) / getClusterNodesNum(cluster2);
            if (destLoad + averageLoad(list2) > CAPACITY_THRESHOLD) {
                logger.debug("Decision: do not move {} to {} because it will be overload", streamToBeMoved, cluster2);
            }
            logger.debug("Decision: move {} to {}", streamToBeMoved, cluster2);
            Map<String, StreamFlow> tmpMap = pm.clusterMap.get(streamToBeMoved);
            if (tmpMap == null) {
                tmpMap = Maps.newHashMap();
                pm.clusterMap.put(streamToBeMoved, tmpMap);
            }
            tmpMap.put(cluster2, new StreamFlow(cluster2));
        }
    }

    public PEClusterMapper analyze() {
        PEClusterMapper pm = new PEClusterMapper();
        int i = 0, j = orderedClusters.size() - 1;
        while (i < j) {
            if (mapStats.get(orderedClusters.get(j).name) != null
                    && mapStats.get(orderedClusters.get(j).name).size() < 2) {
                j--;
                continue;
            }
            checkClusterPair(pm, orderedClusters.get(j).name, orderedClusters.get(i).name);
            j--;
            i++;
        }
        if (pm.clusterMap.size() > 0) {
            return pm;
        } else {
            return null;
        }
    }

    public void printOrderedClusters() throws IOException {
        for (ClusterStats s : orderedClusters) {
            System.out.println(s.name + " - " + s.load);
        }
    }

    public void initData() throws IOException {
        URL url = new URL(statsUrl);
        URLConnection connection = url.openConnection();
        InputStreamReader stream = new InputStreamReader(connection.getInputStream());
        BufferedReader in = new BufferedReader(stream);

        String line;
        String clusterName = "";
        String streamName = "";
        List<PeLoadStat> lastList = null;
        int i = 0;
        boolean bAllZero = true;
        List<String> content = new ArrayList<String>();
        while ((line = in.readLine()) != null) {
            content.add(line);
        }
        in.close();
        logger.debug("file size: " + content.size() + " lines");
        for (String dataLine : content) {
            // dataLine =
            // "S4-cluster3-0.TopicSeen.TopicCountAndReportPE-pe-processing-time.count,2014-01-03 15:52:00,39368407.0";
            // logger.debug(dataLine);
            Pattern pat = Pattern.compile("S4\\-([^\\-]+)\\-\\d+\\.([^\\.]+)\\.[^.]+\\.([^,]+),[^,]+,(.*)$");
            Matcher m = pat.matcher(dataLine);
            if (!m.matches()) {
                mapStats.clear();
                return;
            }
            if (!clusterName.equals(m.group(1)) || !streamName.equals(m.group(2))) {
                i = 0;
                if (bAllZero && lastList != null) {
                    mapStats.get(clusterName).remove(streamName);
                    logger.debug("delete all zero list. {} - {}", clusterName, streamName);
                }
                clusterName = m.group(1);
                streamName = m.group(2);
                logger.debug("newlist. {} - {}", clusterName, streamName);
                bAllZero = true;
                Map<String, List<PeLoadStat>> tmpStreamMap = mapStats.get(clusterName);
                if (tmpStreamMap == null) {
                    tmpStreamMap = Maps.newHashMap();
                    mapStats.put(clusterName, tmpStreamMap);
                }
                lastList = tmpStreamMap.get(streamName);
                if (lastList == null) {
                    lastList = new ArrayList<PeLoadStat>();
                    tmpStreamMap.put(streamName, lastList);
                }
            }

            PeLoadStat tmp;
            if (i < lastList.size()) {
                tmp = lastList.get(i);
            } else {
                tmp = new PeLoadStat();
                lastList.add(tmp);
            }
            i++;
            if (m.group(3).equals("mean")) {
                try {
                    tmp.procTime = (tmp.procTime * tmp.procRound + Double.parseDouble(m.group(4)))
                            / (tmp.procRound + 1);
                    tmp.procRound++;
                } catch (NumberFormatException e) {
                    tmp.procTime = 0.0;
                }
                if (tmp.procTime != 0.0) {
                    bAllZero = false;
                }
            } else {
                try {
                    tmp.eventsCount = (tmp.eventsCount * tmp.eventsRound + Double.parseDouble(m.group(4)) * 6)
                            / (tmp.eventsRound + 1);
                    tmp.eventsRound++;
                } catch (NumberFormatException e) {
                    tmp.eventsCount = 0;
                }
                if (tmp.eventsCount != 0) {
                    bAllZero = false;
                }
            }
        }
        if (bAllZero && lastList != null) {
            mapStats.get(clusterName).remove(streamName);
            logger.debug("delete all zero list. {} - {}", clusterName, streamName);
        }

    }

    public void printData() throws IOException {
        for (Map.Entry<String, Map<String, List<PeLoadStat>>> entry : mapStats.entrySet()) {
            System.out.println("=" + entry.getKey() + "=");
            for (Map.Entry<String, List<PeLoadStat>> listEntry : entry.getValue().entrySet()) {
                System.out.println("--" + listEntry.getKey());
                for (PeLoadStat stat : listEntry.getValue()) {
                    System.out.print("(" + stat.procTime + "," + stat.eventsCount + ",");
                    System.out.print(stat.procRound + "," + stat.eventsRound + "), ");
                }
                System.out.println();
            }
        }
    }
}
