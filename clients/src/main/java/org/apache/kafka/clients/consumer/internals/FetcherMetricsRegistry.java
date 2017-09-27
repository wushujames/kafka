/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.clients.consumer.internals;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.MetricNameTemplate;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;

public class FetcherMetricsRegistry {

    final static String METRIC_GROUP_NAME = "consumer-fetch-manager-metrics";
    
    private final List<MetricNameTemplate> allTemplates;

    private MetricName fetchSizeAvg;
    private MetricName fetchSizeMax;
    private MetricName bytesConsumedRate;
    private MetricName bytesConsumedTotal;
    private MetricName recordsPerRequestAvg;
    private MetricName recordsConsumedRate;
    private MetricName recordsConsumedTotal;
    private MetricName fetchLatencyAvg;
    private MetricName fetchLatencyMax;
    private MetricName fetchRequestRate;
    private MetricName fetchRequestTotal;
    private MetricName recordsLagMax;
    private MetricName fetchThrottleTimeAvg;
    private MetricName fetchThrottleTimeMax;

    private MetricNameTemplate topicFetchSizeAvg;
    private MetricNameTemplate topicFetchSizeMax;
    private MetricNameTemplate topicBytesConsumedRate;
    private MetricNameTemplate topicBytesConsumedTotal;
    private MetricNameTemplate topicRecordsPerRequestAvg;
    private MetricNameTemplate topicRecordsConsumedRate;
    private MetricNameTemplate topicRecordsConsumedTotal;
    private MetricNameTemplate partitionRecordsLag;
    private MetricNameTemplate partitionRecordsLagMax;
    private MetricNameTemplate partitionRecordsLagAvg;

    private Metrics metrics;
    private Set<String> tags;
    private HashSet<String> topicTags;

    public FetcherMetricsRegistry(Metrics metrics) {
        this.metrics = metrics;
        this.tags = this.metrics.config().tags().keySet();
        this.allTemplates = new ArrayList<MetricNameTemplate>();

        /***** Client level *****/
        this.fetchSizeAvg = createMetricName("fetch-size-avg", "The average number of bytes fetched per request", 
                tags);

        this.fetchSizeMax = createMetricName("fetch-size-max", "The maximum number of bytes fetched per request", 
                tags);
        this.bytesConsumedRate = createMetricName("bytes-consumed-rate", "The average number of bytes consumed per second", 
                tags);
        this.bytesConsumedTotal = createMetricName("bytes-consumed-total", "The total number of bytes consumed",
                tags);

        this.recordsPerRequestAvg = createMetricName("records-per-request-avg", "The average number of records in each request", 
                tags);
        this.recordsConsumedRate = createMetricName("records-consumed-rate", "The average number of records consumed per second", 
                tags);
        this.recordsConsumedTotal = createMetricName("records-consumed-total", "The total number of records consumed",
                tags);

        this.fetchLatencyAvg = createMetricName("fetch-latency-avg", "The average time taken for a fetch request.", 
                tags);
        this.fetchLatencyMax = createMetricName("fetch-latency-max", "The max time taken for any fetch request.", 
                tags);
        this.fetchRequestRate = createMetricName("fetch-rate", "The number of fetch requests per second.", 
                tags);
        this.fetchRequestTotal = createMetricName("fetch-total", "The total number of fetch requests.",
                tags);

        this.recordsLagMax = createMetricName("records-lag-max", "The maximum lag in terms of number of records for any partition in this window", 
                tags);

        this.fetchThrottleTimeAvg = createMetricName("fetch-throttle-time-avg", "The average throttle time in ms", 
                tags);
        this.fetchThrottleTimeMax = createMetricName("fetch-throttle-time-max", "The maximum throttle time in ms", 
                tags);

        /***** Topic level *****/
        this.topicTags = new HashSet<>(tags);
        this.topicTags.add("topic");

        this.topicFetchSizeAvg = createTemplate("fetch-size-avg", "The average number of bytes fetched per request for a topic", 
                topicTags);
        this.topicFetchSizeMax = createTemplate("fetch-size-max", "The maximum number of bytes fetched per request for a topic", 
                topicTags);
        this.topicBytesConsumedRate = createTemplate("bytes-consumed-rate", "The average number of bytes consumed per second for a topic", 
                topicTags);
        this.topicBytesConsumedTotal = createTemplate("bytes-consumed-total", "The total number of bytes consumed for a topic",
                topicTags);

        this.topicRecordsPerRequestAvg = createTemplate("records-per-request-avg", "The average number of records in each request for a topic", 
                topicTags);
        this.topicRecordsConsumedRate = createTemplate("records-consumed-rate", "The average number of records consumed per second for a topic", 
                topicTags);
        this.topicRecordsConsumedTotal = createTemplate("records-consumed-total", "The total number of records consumed for a topic",
                topicTags);
        
        /***** Partition level *****/
        this.partitionRecordsLag = createTemplate("{topic}-{partition}.records-lag", "The latest lag of the partition", 
                tags);
        this.partitionRecordsLagMax = createTemplate("{topic}-{partition}.records-lag-max", "The max lag of the partition", 
                tags);
        this.partitionRecordsLagAvg = createTemplate("{topic}-{partition}.records-lag-avg", "The average lag of the partition", 
                tags);
        
    }
    
    public MetricName getFetchSizeAvg() {
        return this.fetchSizeAvg;
    }
    
    public MetricName getFetchSizeMax() {
        return this.fetchSizeMax;
    }
    
    public MetricName getBytesConsumedRate() {
        return this.bytesConsumedRate;
    }
    
    public MetricName getBytesConsumedTotal() {
        return this.bytesConsumedTotal;
    }

    public MetricName getRecordsPerRequestAvg() {
        return this.recordsPerRequestAvg;
    }

    public MetricName getRecordsConsumedRate() {
        return this.recordsConsumedRate;
    }

    public MetricName getRecordsConsumedTotal() {
        return this.recordsConsumedTotal;
    }

    public MetricName getFetchLatencyAvg() {
        return this.fetchLatencyAvg;
    }

    public MetricName getFetchLatencyMax() {
        return this.fetchLatencyMax;
    }

    public MetricName getFetchRequestRate() {
        return this.fetchRequestRate;
    }

    public MetricName getFetchRequestTotal() {
        return this.fetchRequestTotal;
    }

    public MetricName getRecordsLagMax() {
        return this.recordsLagMax;
    }

    public MetricName getFetchThrottleTimeAvg() {
        return this.fetchThrottleTimeAvg;
    }

    public MetricName getFetchThrottleTimeMax() {
        return this.fetchThrottleTimeMax;
    }

    public MetricName getTopicFetchSizeAvg(Map<String, String> metricTags) {
        return metrics.metricInstance(this.topicFetchSizeAvg, metricTags);
    }

    public MetricName getTopicFetchSizeMax(Map<String, String> metricTags) {
        return metrics.metricInstance(this.topicFetchSizeMax, metricTags);
    }

    public MetricName getTopicBytesConsumedRate(Map<String, String> metricTags) {
        return metrics.metricInstance(this.topicBytesConsumedRate, metricTags);
    }

    public MetricName getTopicBytesConsumedTotal(Map<String, String> metricTags) {
        return metrics.metricInstance(this.topicBytesConsumedTotal, metricTags);
    }

    public MetricName getTopicRecordsPerRequestAvg(Map<String, String> metricTags) {
        return metrics.metricInstance(this.topicRecordsPerRequestAvg, metricTags);
    }

    public MetricName getTopicRecordsConsumedRate(Map<String, String> metricTags) {
        return metrics.metricInstance(this.topicRecordsConsumedRate, metricTags);
    }

    public MetricName getTopicRecordsConsumedTotal(Map<String, String> metricTags) {
        return metrics.metricInstance(this.topicRecordsConsumedTotal, metricTags);
    }

    public MetricName getPartitionRecordsLag(String partitionLagMetricName) {
        return metrics.metricName(partitionLagMetricName,
                partitionRecordsLag.group(),
                partitionRecordsLag.description());
    }

    public MetricName getPartitionRecordsLagMax(String partitionLagMetricName) {
        return metrics.metricName(partitionLagMetricName + "-max",
                partitionRecordsLagMax.group(),
                partitionRecordsLagMax.description());
    }

    public MetricName getPartitionRecordsLagAvg(String partitionLagMetricName) {
        return metrics.metricName(partitionLagMetricName + "-avg",
                partitionRecordsLagAvg.group(),
                partitionRecordsLagAvg.description());
    }

    public List<MetricNameTemplate> allTemplates() {
        return allTemplates;
    }
    
    public Sensor sensor(String name) {
        return this.metrics.sensor(name);
    }

    public Sensor getSensor(String name) {
        return this.metrics.getSensor(name);
    }

    public void removeSensor(String name) {
        this.metrics.removeSensor(name);
    }

    private MetricName createMetricName(String name, String description, Set<String> tags) {
        return this.metrics.metricInstance(createTemplate(name, description, this.tags));
    }

    
    private MetricNameTemplate createTemplate(String name, String description, Set<String> tags) {
        MetricNameTemplate template = new MetricNameTemplate(name, METRIC_GROUP_NAME, description, tags);
        this.allTemplates.add(template);
        return template;
    }

}
