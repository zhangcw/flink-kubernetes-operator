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

package org.apache.flink.autoscaler.metrics;

import org.apache.flink.autoscaler.JobAutoScalerContext;
import org.apache.flink.autoscaler.JobAutoScalerImpl;
import org.apache.flink.autoscaler.config.AutoScalerOptions;
import org.apache.flink.autoscaler.utils.HttpUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.kubernetes.configuration.KubernetesConfigOptions;
import org.apache.flink.kubernetes.shaded.com.fasterxml.jackson.databind.ObjectMapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.time.Instant;
import java.util.*;

/**
 * Enum representing the collected Flink metrics for autoscaling. The actual metric names depend on
 * the JobGraph.
 */
public class ExternalMetricUtils {

    private static final Logger LOG = LoggerFactory.getLogger(JobAutoScalerImpl.class);

    private static final ObjectMapper mapper = new ObjectMapper();

    private static final String DateTimeFormat = "yyyy-MM-dd HH:mm:ss";
    private static final String DateFormat = "yyyyMMdd";
    private static final SimpleDateFormat dateTimeFormat = new SimpleDateFormat(DateTimeFormat);
    private static final SimpleDateFormat dateFormat = new SimpleDateFormat(DateFormat);

    public static ExternalCpuUsageMetrics getCpuUsage(
            JobAutoScalerContext context, Instant startTime, Instant endTime) {
        Configuration conf = context.getConfiguration();
        String jobId = conf.get(KubernetesConfigOptions.CLUSTER_ID);
        String httpUrl = conf.get(AutoScalerOptions.EXTERNAL_CPU_METRICS_QUERY_URL);
        String sqlTemplate = conf.get(AutoScalerOptions.EXTERNAL_CPU_METRICS_QUERY_SQL_TEMPLATE);
        String tableName = conf.get(AutoScalerOptions.EXTERNAL_CPU_METRICS_QUERY_TABLE);
        String preferStorage =
                conf.get(AutoScalerOptions.EXTERNAL_CPU_METRICS_QUERY_TABLE_PREFER_STORAGE);
        String appCode = conf.get(AutoScalerOptions.EXTERNAL_CPU_METRICS_QUERY_APP_CODE);
        String appSecret = conf.get(AutoScalerOptions.EXTERNAL_CPU_METRICS_QUERY_APP_SECRET);
        // 没有配置查询url的情况下返回null，不抛出异常
        if (httpUrl.isEmpty()) {
            return null;
        }

        Date startDateTime = Date.from(startTime);
        Date endDateTime = Date.from(endTime);
        TimeZone tz = TimeZone.getTimeZone(System.getenv().getOrDefault("TZ", "UTC"));
        dateTimeFormat.setTimeZone(tz);
        dateFormat.setTimeZone(tz);

        String sql =
                String.format(
                        sqlTemplate,
                        tableName,
                        dateFormat.format(startDateTime),
                        dateFormat.format(endDateTime),
                        dateTimeFormat.format(startDateTime),
                        dateTimeFormat.format(endDateTime),
                        jobId);
        Map<String, String> bodyMap = new HashMap<>();
        bodyMap.put("app_code", appCode);
        bodyMap.put("app_secret", appSecret);
        bodyMap.put("prefer_storage", preferStorage);
        bodyMap.put("sql", sql);
        try {
            String body = mapper.writeValueAsString(bodyMap);
            LOG.info("===============>>> httpUrl: {} body: {}", httpUrl, body);
            String resp = HttpUtils.post(httpUrl, body);
            LOG.info("===============>>> resp: {}", resp);
            ExternalCpuUsageMetrics cpuUsage = ExternalCpuUsageMetrics.fromQueryData(resp);
            LOG.info("===============>>> cpuUsage: {}", cpuUsage);
            return cpuUsage;
        } catch (Exception e) {
            LOG.error("Failed to parse the response of query cpu metrics from external url", e);
            return null;
        }
    }
}
