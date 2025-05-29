/*
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

package org.apache.flink.kubernetes.operator.utils;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import io.fabric8.kubernetes.api.model.ListOptions;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.Watcher;
import io.fabric8.kubernetes.client.WatcherException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/** Util to cache flink jobmanager info. */
public final class JobManagerInfoCache {

    private static final Logger LOG = LoggerFactory.getLogger(JobManagerInfoCache.class);
    private static final Cache<String, JobManagerInfo> cache =
            CacheBuilder.newBuilder()
                    .recordStats()
                    .expireAfterAccess(1, TimeUnit.DAYS)
                    .maximumSize(50000)
                    .build();

    public static String getPodIp(String clusterId) {
        return Optional.ofNullable(get(clusterId)).map(JobManagerInfo::getIp).orElse(null);
    }

    public static String getPodIpIfPresent(String clusterId) {
        return Optional.ofNullable(cache.getIfPresent(clusterId))
                .map(JobManagerInfo::getIp)
                .orElse(null);
    }

    public static String getPodName(String clusterId) {
        return Optional.ofNullable(get(clusterId)).map(JobManagerInfo::getName).orElse(null);
    }

    public static void set(String clusterId, JobManagerInfo jm) {
        if (jm != null) {
            cache.put(clusterId, jm);
        }
    }

    public static JobManagerInfo get(String clusterId) {
        JobManagerInfo jm = cache.getIfPresent(clusterId);
        if (jm == null) {
            LOG.warn("Could not get jobmanager info from cache for cluster {}", clusterId);
        }
        return jm;
    }

    public static void watchFlinkJobManagerPod(
            KubernetesClient kubernetesClient, Set<String> namespaces) {
        Map<String, String> labels = new HashMap<>();
        labels.put("type", "flink-native-kubernetes");
        labels.put("component", "jobmanager");
        labels.put("deployMode", "flink-k8s-operator");
        ListOptions listOptions = new ListOptions();
        listOptions.setResourceVersion("0");

        for (String namespace : namespaces) {
            LOG.info("List and watch jobmanager pod of namespace '{}'", namespace);

            kubernetesClient
                    .pods()
                    .inNamespace(namespace)
                    .withLabels(labels)
                    .watch(
                            listOptions,
                            new Watcher<>() {
                                @Override
                                public void eventReceived(Action action, Pod pod) {
                                    String clusterId = pod.getMetadata().getLabels().get("app");
                                    switch (action) {
                                        case ADDED:
                                        case MODIFIED:
                                            String podIp = pod.getStatus().getPodIP();
                                            String podName = pod.getMetadata().getName();
                                            if (podIp != null
                                                    && !podIp.equals(
                                                            getPodIpIfPresent(clusterId))) {
                                                JobManagerInfo jm =
                                                        new JobManagerInfo(podName, podIp);
                                                LOG.info(
                                                        "UPDATE jobmanager info cache: {}: {}",
                                                        clusterId,
                                                        jm);
                                                set(clusterId, jm);
                                            }
                                            break;
                                        case DELETED:
                                            LOG.info("DELETE jobmanager info cache: {}", clusterId);
                                            cache.invalidate(clusterId);
                                            break;
                                        default:
                                            break;
                                    }
                                }

                                @Override
                                public void onClose(WatcherException cause) {
                                    LOG.info("jobmanager pod watch onClose: " + cause);
                                }
                            });
        }
    }
}
