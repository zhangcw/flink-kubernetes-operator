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

import org.apache.flink.autoscaler.JobAutoScalerImpl;
import org.apache.flink.kubernetes.shaded.com.fasterxml.jackson.databind.ObjectMapper;

import lombok.AllArgsConstructor;
import lombok.Data;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Enum representing the collected Flink metrics for autoscaling. The actual metric names depend on
 * the JobGraph.
 */
@Data
@AllArgsConstructor
public class ExternalCpuUsageMetrics {
    private static final Logger LOG = LoggerFactory.getLogger(JobAutoScalerImpl.class);

    private static final ObjectMapper mapper = new ObjectMapper();
    private Double cpuUsage;
    private Double cpuLimit;

    public static ExternalCpuUsageMetrics fromQueryData(String queryData) {
        try {
            Map<String, Map<String, List<Map<String, Double>>>> dataMap =
                    mapper.readValue(queryData, Map.class);
            Map<String, Double> data = dataMap.get("data").get("list").get(0);
            return new ExternalCpuUsageMetrics(data.get("cpu_usage"), data.get("cpu_limit"));
        } catch (Exception e) {
            LOG.warn("Failed to parse the query data of cpu metrics from external url", e);
            return null;
        }
    }
}
