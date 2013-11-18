package org.apache.s4.base.util;

import com.codahale.metrics.MetricRegistry;

public class S4MetricsRegistry {
    private final static MetricRegistry mr = new MetricRegistry();

    public static MetricRegistry getMr() {
        return mr;
    }
}
