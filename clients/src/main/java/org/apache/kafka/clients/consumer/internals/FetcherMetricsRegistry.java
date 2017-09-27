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

    private MetricNameTemplate fetchSizeAvg;
    private MetricNameTemplate fetchSizeMax;
    private MetricNameTemplate bytesConsumedRate;
    private MetricNameTemplate bytesConsumedTotal;
    private MetricNameTemplate recordsPerRequestAvg;
    private MetricNameTemplate recordsConsumedRate;
    private MetricNameTemplate recordsConsumedTotal;
    private MetricNameTemplate fetchLatencyAvg;
    private MetricNameTemplate fetchLatencyMax;
    private MetricNameTemplate fetchRequestRate;
    private MetricNameTemplate fetchRequestTotal;
    private MetricNameTemplate recordsLagMax;
    private MetricNameTemplate fetchThrottleTimeAvg;
    private MetricNameTemplate fetchThrottleTimeMax;
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
        this.fetchSizeAvg = createTemplate("fetch-size-avg", METRIC_GROUP_NAME, 
                "The average number of bytes fetched per request", tags);

        this.fetchSizeMax = createTemplate("fetch-size-max", METRIC_GROUP_NAME, 
                "The maximum number of bytes fetched per request", tags);
        this.bytesConsumedRate = createTemplate("bytes-consumed-rate", METRIC_GROUP_NAME, 
                "The average number of bytes consumed per second", tags);
        this.bytesConsumedTotal = createTemplate("bytes-consumed-total", METRIC_GROUP_NAME,
                "The total number of bytes consumed", tags);

        this.recordsPerRequestAvg = createTemplate("records-per-request-avg", METRIC_GROUP_NAME, 
                "The average number of records in each request", tags);
        this.recordsConsumedRate = createTemplate("records-consumed-rate", METRIC_GROUP_NAME, 
                "The average number of records consumed per second", tags);
        this.recordsConsumedTotal = createTemplate("records-consumed-total", METRIC_GROUP_NAME,
                "The total number of records consumed", tags);

        this.fetchLatencyAvg = createTemplate("fetch-latency-avg", METRIC_GROUP_NAME, 
                "The average time taken for a fetch request.", tags);
        this.fetchLatencyMax = createTemplate("fetch-latency-max", METRIC_GROUP_NAME, 
                "The max time taken for any fetch request.", tags);
        this.fetchRequestRate = createTemplate("fetch-rate", METRIC_GROUP_NAME, 
                "The number of fetch requests per second.", tags);
        this.fetchRequestTotal = createTemplate("fetch-total", METRIC_GROUP_NAME,
                "The total number of fetch requests.", tags);

        this.recordsLagMax = createTemplate("records-lag-max", METRIC_GROUP_NAME, 
                "The maximum lag in terms of number of records for any partition in this window", tags);

        this.fetchThrottleTimeAvg = createTemplate("fetch-throttle-time-avg", METRIC_GROUP_NAME, 
                "The average throttle time in ms", tags);
        this.fetchThrottleTimeMax = createTemplate("fetch-throttle-time-max", METRIC_GROUP_NAME, 
                "The maximum throttle time in ms", tags);

        /***** Topic level *****/
        this.topicTags = new HashSet<>(tags);
        this.topicTags.add("topic");

        this.topicFetchSizeAvg = createTemplate("fetch-size-avg", METRIC_GROUP_NAME, 
                "The average number of bytes fetched per request for a topic", topicTags);
        this.topicFetchSizeMax = createTemplate("fetch-size-max", METRIC_GROUP_NAME, 
                "The maximum number of bytes fetched per request for a topic", topicTags);
        this.topicBytesConsumedRate = createTemplate("bytes-consumed-rate", METRIC_GROUP_NAME, 
                "The average number of bytes consumed per second for a topic", topicTags);
        this.topicBytesConsumedTotal = createTemplate("bytes-consumed-total", METRIC_GROUP_NAME,
                "The total number of bytes consumed for a topic", topicTags);

        this.topicRecordsPerRequestAvg = createTemplate("records-per-request-avg", METRIC_GROUP_NAME, 
                "The average number of records in each request for a topic", topicTags);
        this.topicRecordsConsumedRate = createTemplate("records-consumed-rate", METRIC_GROUP_NAME, 
                "The average number of records consumed per second for a topic", topicTags);
        this.topicRecordsConsumedTotal = createTemplate("records-consumed-total", METRIC_GROUP_NAME,
                "The total number of records consumed for a topic", topicTags);
        
        /***** Partition level *****/
        this.partitionRecordsLag = createTemplate("{topic}-{partition}.records-lag", METRIC_GROUP_NAME, 
                "The latest lag of the partition", tags);
        this.partitionRecordsLagMax = createTemplate("{topic}-{partition}.records-lag-max", METRIC_GROUP_NAME, 
                "The max lag of the partition", tags);
        this.partitionRecordsLagAvg = createTemplate("{topic}-{partition}.records-lag-avg", METRIC_GROUP_NAME, 
                "The average lag of the partition", tags);
        
    }
    
    public MetricName getFetchSizeAvg() {
        return metrics.metricInstance(this.fetchSizeAvg);
    }
    
    public MetricName getFetchSizeMax() {
        return metrics.metricInstance(this.fetchSizeMax);
    }
    
    public MetricName getBytesConsumedRate() {
        return metrics.metricInstance(this.bytesConsumedRate);
    }
    
    public MetricName getBytesConsumedTotal() {
        return metrics.metricInstance(this.bytesConsumedTotal);
    }

    public MetricName getRecordsPerRequestAvg() {
        return metrics.metricInstance(this.recordsPerRequestAvg);
    }

    public MetricName getRecordsConsumedRate() {
        return metrics.metricInstance(this.recordsConsumedRate);
    }

    public MetricName getRecordsConsumedTotal() {
        return metrics.metricInstance(this.recordsConsumedTotal);
    }

    public MetricName getFetchLatencyAvg() {
        return metrics.metricInstance(this.fetchLatencyAvg);
    }

    public MetricName getFetchLatencyMax() {
        return metrics.metricInstance(this.fetchLatencyMax);
    }

    public MetricName getFetchRequestRate() {
        return metrics.metricInstance(this.fetchRequestRate);
    }

    public MetricName getFetchRequestTotal() {
        return metrics.metricInstance(this.fetchRequestTotal);
    }

    public MetricName getRecordsLagMax() {
        return metrics.metricInstance(this.recordsLagMax);
    }

    public MetricName getFetchThrottleTimeAvg() {
        return metrics.metricInstance(this.fetchThrottleTimeAvg);
    }

    public MetricName getFetchThrottleTimeMax() {
        return metrics.metricInstance(this.fetchThrottleTimeMax);
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

    private MetricNameTemplate createTemplate(String name, String group, String description, Set<String> tags) {
        MetricNameTemplate template = new MetricNameTemplate(name, group, description, tags);
        this.allTemplates.add(template);
        return template;
    }

}
