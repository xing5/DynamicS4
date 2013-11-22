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

package org.apache.s4.example.twitter;

import org.apache.s4.core.App;
import org.apache.s4.base.Event;
import org.apache.s4.core.DynamicProcessingElement;
import org.apache.s4.core.RemoteStream;
//import org.apache.s4.core.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// keyed by topic name
public class TopicCountAndReportPE extends DynamicProcessingElement {

    transient RemoteStream downStream;
    //transient Stream<TopicEvent> downStream;
    transient int threshold = 1;
    int count;
    boolean firstEvent = true;

    static Logger logger = LoggerFactory.getLogger(TopicCountAndReportPE.class);

    public TopicCountAndReportPE() {
        // required for checkpointing in S4 0.5. Requirement to be removed in 0.6
    }

    public TopicCountAndReportPE(App app) {
        super(app);
    }

    public void setDownstream(RemoteStream aggregatedTopicStream) {
        this.downStream = aggregatedTopicStream;
    }
    
    public RemoteStream getDownstream() {
        return downStream;
    }

    public void onEvent(Event event) {
        if (firstEvent) {
            logger.info("Handling new topic [{}]", getId());
            firstEvent = false;
        }
        //logger.info("Get new topic [{}], count [{}]", getId(), count);
        count += event.get("count", int.class);
        Event topicEvent = new Event();
        topicEvent.put("topic", String.class, getId());
        topicEvent.put("count", int.class, count);
        downStream.put(topicEvent);
    }

    @Override
    protected void onCreate() {
        // TODO Auto-generated method stub

    }

    public void onTime() {
    }

    @Override
    protected void onRemove() {
    }

}
