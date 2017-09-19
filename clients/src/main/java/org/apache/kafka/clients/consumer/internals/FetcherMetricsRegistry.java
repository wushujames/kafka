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

import java.util.Arrays;
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
    
    public MetricNameTemplate fetchSizeAvg;
    public MetricNameTemplate fetchSizeMax;
    public MetricNameTemplate bytesConsumedRate;
    public MetricNameTemplate bytesConsumedTotal;
    public MetricNameTemplate recordsPerRequestAvg;
    public MetricNameTemplate recordsConsumedRate;
    public MetricNameTemplate recordsConsumedTotal;
    public MetricNameTemplate fetchLatencyAvg;
    public MetricNameTemplate fetchLatencyMax;
    public MetricNameTemplate fetchRequestRate;
    public MetricNameTemplate fetchRequestTotal;
    public MetricNameTemplate recordsLagMax;
    public MetricNameTemplate fetchThrottleTimeAvg;
    public MetricNameTemplate fetchThrottleTimeMax;
    public MetricNameTemplate topicFetchSizeAvg;
    public MetricNameTemplate topicFetchSizeMax;
    public MetricNameTemplate topicBytesConsumedRate;
    public MetricNameTemplate topicBytesConsumedTotal;
    public MetricNameTemplate topicRecordsPerRequestAvg;
    public MetricNameTemplate topicRecordsConsumedRate;
    public MetricNameTemplate topicRecordsConsumedTotal;
    public MetricNameTemplate partitionRecordsLag;
    public MetricNameTemplate partitionRecordsLagMax;
    public MetricNameTemplate partitionRecordsLagAvg;

    private Metrics metrics;
    private Set<String> tags;
    private HashSet<String> topicTags;

    public FetcherMetricsRegistry(Metrics metrics) {
        this.metrics = metrics;
        this.tags = this.metrics.config().tags().keySet();
        
        /***** Client level *****/
        this.fetchSizeAvg = new MetricNameTemplate("fetch-size-avg", METRIC_GROUP_NAME, 
                "The average number of bytes fetched per request", tags);

        this.fetchSizeMax = new MetricNameTemplate("fetch-size-max", METRIC_GROUP_NAME, 
                "The maximum number of bytes fetched per request", tags);
        this.bytesConsumedRate = new MetricNameTemplate("bytes-consumed-rate", METRIC_GROUP_NAME, 
                "The average number of bytes consumed per second", tags);
        this.bytesConsumedTotal = new MetricNameTemplate("bytes-consumed-total", METRIC_GROUP_NAME,
                "The total number of bytes consumed", tags);

        this.recordsPerRequestAvg = new MetricNameTemplate("records-per-request-avg", METRIC_GROUP_NAME, 
                "The average number of records in each request", tags);
        this.recordsConsumedRate = new MetricNameTemplate("records-consumed-rate", METRIC_GROUP_NAME, 
                "The average number of records consumed per second", tags);
        this.recordsConsumedTotal = new MetricNameTemplate("records-consumed-total", METRIC_GROUP_NAME,
                "The total number of records consumed", tags);

        this.fetchLatencyAvg = new MetricNameTemplate("fetch-latency-avg", METRIC_GROUP_NAME, 
                "The average time taken for a fetch request.", tags);
        this.fetchLatencyMax = new MetricNameTemplate("fetch-latency-max", METRIC_GROUP_NAME, 
                "The max time taken for any fetch request.", tags);
        this.fetchRequestRate = new MetricNameTemplate("fetch-rate", METRIC_GROUP_NAME, 
                "The number of fetch requests per second.", tags);
        this.fetchRequestTotal = new MetricNameTemplate("fetch-total", METRIC_GROUP_NAME,
                "The total number of fetch requests.", tags);

        this.recordsLagMax = new MetricNameTemplate("records-lag-max", METRIC_GROUP_NAME, 
                "The maximum lag in terms of number of records for any partition in this window", tags);

        this.fetchThrottleTimeAvg = new MetricNameTemplate("fetch-throttle-time-avg", METRIC_GROUP_NAME, 
                "The average throttle time in ms", tags);
        this.fetchThrottleTimeMax = new MetricNameTemplate("fetch-throttle-time-max", METRIC_GROUP_NAME, 
                "The maximum throttle time in ms", tags);

        /***** Topic level *****/
        this.topicTags = new HashSet<>(tags);
        this.topicTags.add("topic");

        this.topicFetchSizeAvg = new MetricNameTemplate("fetch-size-avg", METRIC_GROUP_NAME, 
                "The average number of bytes fetched per request for a topic", topicTags);
        this.topicFetchSizeMax = new MetricNameTemplate("fetch-size-max", METRIC_GROUP_NAME, 
                "The maximum number of bytes fetched per request for a topic", topicTags);
        this.topicBytesConsumedRate = new MetricNameTemplate("bytes-consumed-rate", METRIC_GROUP_NAME, 
                "The average number of bytes consumed per second for a topic", topicTags);
        this.topicBytesConsumedTotal = new MetricNameTemplate("bytes-consumed-total", METRIC_GROUP_NAME,
                "The total number of bytes consumed for a topic", topicTags);

        this.topicRecordsPerRequestAvg = new MetricNameTemplate("records-per-request-avg", METRIC_GROUP_NAME, 
                "The average number of records in each request for a topic", topicTags);
        this.topicRecordsConsumedRate = new MetricNameTemplate("records-consumed-rate", METRIC_GROUP_NAME, 
                "The average number of records consumed per second for a topic", topicTags);
        this.topicRecordsConsumedTotal = new MetricNameTemplate("records-consumed-total", METRIC_GROUP_NAME,
                "The total number of records consumed for a topic", topicTags);
        
        /***** Partition level *****/
        this.partitionRecordsLag = new MetricNameTemplate("{topic}-{partition}.records-lag", METRIC_GROUP_NAME, 
                "The latest lag of the partition", tags);
        this.partitionRecordsLagMax = new MetricNameTemplate("{topic}-{partition}.records-lag-max", METRIC_GROUP_NAME, 
                "The max lag of the partition", tags);
        this.partitionRecordsLagAvg = new MetricNameTemplate("{topic}-{partition}.records-lag-avg", METRIC_GROUP_NAME, 
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

    public List<MetricNameTemplate> getAllTemplates() {
        return Arrays.asList(
            fetchSizeAvg,
            fetchSizeMax,
            bytesConsumedRate,
            bytesConsumedTotal,
            recordsPerRequestAvg,
            recordsConsumedRate,
            recordsConsumedTotal,
            fetchLatencyAvg,
            fetchLatencyMax,
            fetchRequestRate,
            fetchRequestTotal,
            recordsLagMax,
            fetchThrottleTimeAvg,
            fetchThrottleTimeMax,
            topicFetchSizeAvg,
            topicFetchSizeMax,
            topicBytesConsumedRate,
            topicBytesConsumedTotal,
            topicRecordsPerRequestAvg,
            topicRecordsConsumedRate,
            topicRecordsConsumedTotal,
            partitionRecordsLag,
            partitionRecordsLagAvg,
            partitionRecordsLagMax
        );
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



}
