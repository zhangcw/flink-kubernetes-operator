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

import org.apache.flink.kubernetes.utils.KubernetesUtils;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.Watcher;
import io.fabric8.kubernetes.client.WatcherException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/** Util to cache flink jobmanager info. */
public final class JobManagerIpCache {

    private static final Logger LOG = LoggerFactory.getLogger(JobManagerIpCache.class);
    private static final Cache<String, String> cache =
            CacheBuilder.newBuilder()
                    .recordStats()
                    .expireAfterWrite(1, TimeUnit.DAYS)
                    .maximumSize(20000)
                    .build();

    private static KubernetesClient client;

    public static String get(String clusterId, String namespace) {
        String jmIp = cache.getIfPresent(clusterId);
        if (jmIp == null) {
            LOG.warn(
                    "Could not get jobmanager ip from cache, try to get it with kubernetes client");
            jmIp = getFlinkJobManagerPodIp(clusterId, namespace);
            LOG.info("Got jobmanager ip with kubernetes client: {}", jmIp);
            if (jmIp != null) {
                cache.put(clusterId, jmIp);
            }
        }
        return jmIp;
    }

    public static void set(String clusterId, String jmIp) {
        cache.put(clusterId, jmIp);
    }

    public static void refresh(String clusterId, String namespace) {
        String jmIp = getFlinkJobManagerPodIp(clusterId, namespace);
        cache.put(clusterId, jmIp);
    }

    public static Map<String, String> listFlinkJobManagerPodIps(Set<String> namespaces) {
        Map<String, String> jmIps = new HashMap<>();
        Map<String, String> labels = new HashMap<>();
        labels.put("type", "flink-native-kubernetes");
        labels.put("component", "jobmanager");
        labels.put("deployMode", "flink-k8s-operator");
        for (String ns : namespaces) {
            List<Pod> pods = listFlinkJobManagerPod(ns, labels);
            pods.forEach(
                    p ->
                            jmIps.put(
                                    p.getMetadata().getLabels().get("app"),
                                    p.getStatus().getPodIP()));
        }
        return jmIps;
    }

    public static String getFlinkJobManagerPodIp(String clusterId, String namespace) {
        var labels = KubernetesUtils.getJobManagerSelectors(clusterId);
        List<Pod> pods = listFlinkJobManagerPod(namespace, labels);
        return pods.isEmpty() ? null : pods.get(0).getStatus().getPodIP();
    }

    private static List<Pod> listFlinkJobManagerPod(String namespace, Map<String, String> labels) {
        return client.pods()
                .inNamespace(namespace)
                .withNewFilter()
                .withLabels(labels)
                .endFilter()
                .list()
                .getItems();
    }

    public static void watchFlinkJobManagerPod(
            KubernetesClient kubernetesClient, Set<String> namespaces) {
        Map<String, String> labels = new HashMap<>();
        client = kubernetesClient;
        labels.put("type", "flink-native-kubernetes");
        labels.put("component", "jobmanager");
        labels.put("deployMode", "flink-k8s-operator");
        for (String namespace : namespaces) {
            LOG.info("List and watch jobmanager pod of namespace '{}'", namespace);
            kubernetesClient
                    .pods()
                    .inNamespace(namespace)
                    .withNewFilter()
                    .withLabels(labels)
                    .endFilter()
                    .watch(
                            new Watcher<Pod>() {
                                @Override
                                public void eventReceived(Action action, Pod pod) {
                                    switch (action) {
                                        case ADDED:
                                        case MODIFIED:
                                            String clusterId =
                                                    pod.getMetadata().getLabels().get("app");
                                            String jmIp = pod.getStatus().getPodIP();
                                            if (jmIp != null) {
                                                LOG.info(
                                                        "update jobmanager ip cache: action: {}, pod: {}, ip: {}",
                                                        action,
                                                        clusterId,
                                                        jmIp);
                                                set(clusterId, jmIp);
                                            }
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
