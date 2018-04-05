/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package kafka.authorization.manager.utils.client;

import org.apache.kafka.clients.consumer.internals.AbstractPartitionAssignor;
import org.apache.kafka.clients.consumer.internals.PartitionAssignor;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 *
 */
public class ExclusiveAssignor extends AbstractPartitionAssignor {

    public interface Callback {
        void onSuccess();
    }


    private static Logger LOGGER = LoggerFactory.getLogger(ExclusiveAssignor.class);

    public static String NAME = "exclusive";


    private String leaderId = null;
    private Callback callback = null;

    public void setLeaderId(String leaderId) {
        this.leaderId = leaderId;
    }
    public void setCallBack(Callback callBack){this.callback = callBack;}


    @Override
    public String name() {
        return NAME;
    }

    private Map<String, List<String>> consumersPerTopic(Map<String, List<String>> consumerMetadata) {
        Map<String, List<String>> res = new HashMap<>();
        for (Map.Entry<String, List<String>> subscriptionEntry : consumerMetadata.entrySet()) {
            String consumerId = subscriptionEntry.getKey();
            for (String topic : subscriptionEntry.getValue())
                put(res, topic, consumerId);
        }
        return res;
    }

//    @Override
//    public Map<String, List<TopicPartition>> assign(Map<String, Integer> partitionsPerTopic,
//                                                    Map<String, List<String>> subscriptions) {
//        Map<String, List<String>> consumersPerTopic = consumersPerTopic(subscriptions);
//        Map<String, List<TopicPartition>> assignment = new HashMap<>();
//        for (String memberId : subscriptions.keySet())
//            assignment.put(memberId, new ArrayList<TopicPartition>());
//
//        for (Map.Entry<String, List<String>> topicEntry : consumersPerTopic.entrySet()) {
//            String topic = topicEntry.getKey();
//            List<String> consumersForTopic = topicEntry.getValue();
//
//            Integer numPartitionsForTopic = partitionsPerTopic.get(topic);
//            if (numPartitionsForTopic == null)
//                continue;
//
//            Collections.sort(consumersForTopic);
//
//            int numPartitionsPerConsumer = numPartitionsForTopic / consumersForTopic.size();
//            int consumersWithExtraPartition = numPartitionsForTopic % consumersForTopic.size();
//
//            List<TopicPartition> partitions = AbstractPartitionAssignor.partitions(topic, numPartitionsForTopic);
//            for (int i = 0, n = consumersForTopic.size(); i < n; i++) {
//                int start = numPartitionsPerConsumer * i + Math.min(i, consumersWithExtraPartition);
//                int length = numPartitionsPerConsumer + (i + 1 > consumersWithExtraPartition ? 0 : 1);
//                assignment.get(consumersForTopic.get(i)).addAll(partitions.subList(start, start + length));
//            }
//        }
//        return assignment;
//    }

    @Override
    public Map<String, List<TopicPartition>> assign(Map<String, Integer> partitionsPerTopic,
                                                    Map<String, List<String>> subscriptions) {
        LOGGER.info("perform exclusive assign");
        if(leaderId == null)
            throw new IllegalArgumentException("leaderId should already been set before assign is called");
        if(callback == null)
            throw new IllegalArgumentException("callback should already been set before assign is called");

        List<TopicPartition> allPartitions = new ArrayList<TopicPartition>();
        partitionsPerTopic.forEach((topic, partitionNumber) -> {
            for(int i=0; i < partitionNumber; i++)
                allPartitions.add(new TopicPartition(topic, i));
        });
        Map<String, List<TopicPartition>> assignment = new HashMap<>();
        for (String memberId : subscriptions.keySet()) {
            assignment.put(memberId, new ArrayList<TopicPartition>());
            if(memberId.equals(leaderId)){
                assignment.get(memberId).addAll(allPartitions);
            }
        }
        callback.onSuccess();
        return assignment;
    }

}
