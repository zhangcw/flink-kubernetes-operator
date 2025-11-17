/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.autoscaler;

import org.apache.flink.autoscaler.config.AutoScalerOptions;
import org.apache.flink.autoscaler.metrics.CollectedMetricHistory;
import org.apache.flink.autoscaler.metrics.CollectedMetrics;
import org.apache.flink.autoscaler.metrics.FlinkMetric;
import org.apache.flink.autoscaler.metrics.ScalingMetric;
import org.apache.flink.autoscaler.state.AutoScalerStateStore;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.kubernetes.shaded.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.kubernetes.shaded.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.rest.messages.job.metrics.AggregatedMetric;
import org.apache.flink.runtime.rest.messages.job.metrics.Metric;

import lombok.AllArgsConstructor;
import lombok.Data;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URL;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Instant;
import java.util.*;

import static org.apache.flink.autoscaler.utils.DateTimeUtils.readable;

/** Query job metrics from VictoriaMetrics * */
public class VmScalingMetricCollector<KEY, Context extends JobAutoScalerContext<KEY>>
        extends ScalingMetricCollector<KEY, Context> {
    private static final Logger LOG = LoggerFactory.getLogger(VmScalingMetricCollector.class);

    private Clock clock = Clock.systemDefaultZone();

    private static final String METRIC_PREFIX = "FLINK_SCALING_METRIC_";

    private static final String[] vertexMetricNames =
            new String[] {
                METRIC_PREFIX + "NUM_RECORDS_IN",
                METRIC_PREFIX + "NUM_RECORDS_OUT",
                METRIC_PREFIX + "LOAD",
                METRIC_PREFIX + "LAG",
                METRIC_PREFIX + "OBSERVED_TPR",
                METRIC_PREFIX + "ACCUMULATED_BUSY_TIME"
            };
    private static final String[] globalMetricNames =
            new String[] {
                METRIC_PREFIX + "GC_PRESSURE",
                METRIC_PREFIX + "METASPACE_MEMORY_USED",
                METRIC_PREFIX + "NUM_TASK_SLOTS_USED",
                METRIC_PREFIX + "HEAP_MAX_USAGE_RATIO",
                METRIC_PREFIX + "HEAP_MEMORY_USED",
                METRIC_PREFIX + "MANAGED_MEMORY_USED"
            };

    @Override
    protected Map<FlinkMetric, Metric> queryJmMetrics(Context ctx) throws Exception {
        return null;
    }

    @Override
    protected Map<FlinkMetric, AggregatedMetric> queryTmMetrics(Context ctx) throws Exception {
        return null;
    }

    @Override
    protected Map<JobVertexID, Map<FlinkMetric, AggregatedMetric>> queryAllAggregatedMetrics(
            Context ctx, Map<JobVertexID, Map<String, FlinkMetric>> filteredVertexMetricNames) {
        return null;
    }

    @Data
    private class VmMetricValue {
        @Data
        @AllArgsConstructor
        public class MetricInfo {
            private String name;
            private String jobId;
            private String vertexId;
        }

        @Data
        @AllArgsConstructor
        public class MetricValue {
            private Instant ts;
            private double val;
        }

        private MetricInfo info;
        private List<MetricValue> values = new ArrayList<>();

        public VmMetricValue(JsonNode node) {
            JsonNode metricNode = node.get("metric");
            JsonNode valuesNode = node.get("values");
            info =
                    new MetricInfo(
                            metricNode.get("__name__").asText().replace(METRIC_PREFIX, ""),
                            metricNode.get("job_id").asText(),
                            metricNode.get("vertex_id") == null
                                    ? null
                                    : metricNode.get("vertex_id").asText());
            for (JsonNode vNode : valuesNode) {
                values.add(
                        new MetricValue(
                                Instant.ofEpochMilli((long) (vNode.get(0).asDouble() * 1000)),
                                vNode.get(1).asDouble()));
            }
        }
    }

    public CollectedMetricHistory updateMetrics(
            Context ctx, AutoScalerStateStore<KEY, Context> stateStore) throws Exception {

        var conf = ctx.getConfiguration();

        var now = clock.instant();

        var metricWindowSize = getMetricWindowSize(conf);
        var scaleUpMetricMinWindowSize = getScaleUpMetricMinWindowSize(conf);

        // get job running timestamp
        var jobDetailsInfo =
                getJobDetailsInfo(ctx, conf.get(AutoScalerOptions.FLINK_CLIENT_TIMEOUT));
        var jobRunningTs = getJobRunningTs(jobDetailsInfo);
        var jobName = jobDetailsInfo.getName();

        // query metrics after running and in configured metric window size.
        var queryStartTime = jobRunningTs;
        if (jobRunningTs.isBefore(now.minus(metricWindowSize))) {
            queryStartTime = now.minus(metricWindowSize);
        }
        var metricHistory = getMetricHistory(ctx, jobName, queryStartTime, now);

        var topology = getJobTopology(ctx, stateStore, jobDetailsInfo);
        var stableTime = jobRunningTs.plus(conf.get(AutoScalerOptions.STABILIZATION_INTERVAL));
        final boolean isStabilizing = now.isBefore(stableTime);

        // Calculate timestamp when the metric windows is full
        var windowFullTime =
                getWindowFullTime(metricHistory.tailMap(stableTime), now, metricWindowSize);
        var scaleUpWindowFullTime =
                getWindowFullTime(
                        metricHistory.tailMap(stableTime), now, scaleUpMetricMinWindowSize);
        var collectedMetrics = new CollectedMetricHistory(topology, metricHistory, jobRunningTs);
        if (now.isBefore(scaleUpWindowFullTime)) {
            LOG.info("Scale up metric window not full until {}", readable(scaleUpWindowFullTime));
        } else {
            collectedMetrics.setMinWindowFullyCollected(true);
        }
        if (now.isBefore(windowFullTime)) {
            if (isStabilizing) {
                LOG.info("Stabilizing until {}", readable(stableTime));
            } else {
                LOG.info(
                        "Metric window is not full until {}. {} samples collected so far",
                        readable(windowFullTime),
                        metricHistory.size());
            }
        } else {
            collectedMetrics.setFullyCollected(true);
            LOG.info("Metric window is fully collected");
        }
        return collectedMetrics;
    }

    private SortedMap<Instant, CollectedMetrics> getMetricHistory(
            Context ctx, String jobName, Instant startTime, Instant endTime) {
        SortedMap<Instant, CollectedMetrics> histories = new TreeMap<>();
        var allMetrics = queryVmMetrics(ctx, jobName, startTime, endTime);

        allMetrics.forEach(
                m -> {
                    boolean isVertexMetric = StringUtils.isNotEmpty(m.getInfo().getVertexId());
                    if (isVertexMetric) {
                        JobVertexID vertexID = JobVertexID.fromHexString(m.getInfo().getVertexId());
                        for (VmMetricValue.MetricValue mVal : m.getValues()) {
                            if (!histories.containsKey(mVal.ts)) {
                                histories.put(
                                        mVal.ts,
                                        new CollectedMetrics(new HashMap<>(), new HashMap<>()));
                            }
                            var vMetrics = histories.get(mVal.ts).getVertexMetrics();
                            if (vMetrics.containsKey(vertexID)) {
                                vMetrics.get(vertexID)
                                        .put(ScalingMetric.valueOf(m.getInfo().name), mVal.val);
                            } else {
                                vMetrics.put(
                                        vertexID,
                                        new HashMap<>() {
                                            {
                                                put(
                                                        ScalingMetric.valueOf(m.getInfo().name),
                                                        mVal.val);
                                            }
                                        });
                            }
                        }
                    } else {
                        for (VmMetricValue.MetricValue mVal : m.getValues()) {
                            if (!histories.containsKey(mVal.ts)) {
                                histories.put(
                                        mVal.ts,
                                        new CollectedMetrics(new HashMap<>(), new HashMap<>()));
                            }
                            var gMetrics = histories.get(mVal.ts).getGlobalMetrics();
                            gMetrics.put(ScalingMetric.valueOf(m.getInfo().name), mVal.val);
                        }
                    }
                });
        return histories;
    }

    public List<VmMetricValue> queryVmMetrics(
            Context ctx, String jobName, Instant startTime, Instant endTime) {
        Configuration conf = ctx.getConfiguration();
        String httpUrl = conf.get(AutoScalerOptions.BK_BASE_VM_METRICS_QUERY_URL);
        String appCode = conf.get(AutoScalerOptions.BK_BASE_APP_CODE);
        String appSecret = conf.get(AutoScalerOptions.BK_BASE_APP_SECRET);
        // 没有配置查询url的情况下返回null，不抛出异常
        if (StringUtils.isBlank(httpUrl)) {
            return Collections.emptyList();
        }

        List<String> metricList = new ArrayList<>();
        for (String v : vertexMetricNames) {
            metricList.add(v + "{job_id=\"" + jobName + "\"}");
        }
        for (String g : globalMetricNames) {
            metricList.add(g + "{job_id=\"" + jobName + "\"}");
        }
        String metricStr = "(" + String.join(",", metricList) + ")";
        metricStr = URLEncoder.encode(metricStr, StandardCharsets.UTF_8);

        try {
            ObjectMapper objectMapper = new ObjectMapper();
            long startTimeTs = startTime.toEpochMilli() / 1000;
            long endTimeTs = endTime.toEpochMilli() / 1000;
            String url =
                    String.format(
                            "%s?app_code=%s&app_secret=%s&start=%s&end=%s&step=60s&query=%s",
                            httpUrl,
                            appCode,
                            URLEncoder.encode(appSecret, StandardCharsets.UTF_8),
                            startTimeTs,
                            endTimeTs,
                            metricStr);
            JsonNode jsonNode = objectMapper.readTree(new URL(url));
            List<VmMetricValue> metricValues = new ArrayList<>();
            for (JsonNode node : jsonNode.get("data").get("result")) {
                metricValues.add(new VmMetricValue(node));
            }

            return metricValues;
        } catch (Exception e) {
            LOG.error("Failed to parse the response of query vm metrics from external url", e);
            return Collections.emptyList();
        }
    }
}
