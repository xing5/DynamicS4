/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.s4.comm.util;

import java.util.Map;

import org.apache.s4.base.util.S4MetricsRegistry;
import org.apache.s4.comm.topology.Cluster;

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.google.common.collect.Maps;

public class EmitterMetrics {
    private static final Map<String, Meter> emittersMeters = Maps.newHashMap();

    private Meter emitterMeter;

    public EmitterMetrics(Cluster cluster) {
        String clusterName = cluster.getPhysicalCluster().getName();
        emitterMeter = emittersMeters.get(clusterName);
        if (emitterMeter == null) {
            emitterMeter = S4MetricsRegistry.getMr().meter(MetricRegistry.name("event-emitted", clusterName));
            emittersMeters.put(clusterName, emitterMeter);
        }
    }

    public void sentMessage(int partitionId) {
        emitterMeter.mark();
    }
}
