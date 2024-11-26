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

package org.apache.flink.kubernetes.operator.autoscaler;

import org.apache.flink.autoscaler.JobAutoScaler;
import org.apache.flink.autoscaler.JobAutoScalerImpl;
import org.apache.flink.autoscaler.RestApiMetricsCollector;
import org.apache.flink.autoscaler.ScalingExecutor;
import org.apache.flink.autoscaler.ScalingMetricEvaluator;
import org.apache.flink.autoscaler.event.AutoScalerEventHandler;
import org.apache.flink.autoscaler.jdbc.event.JdbcAutoScalerEventHandler;
import org.apache.flink.autoscaler.jdbc.event.JdbcEventInteractor;
import org.apache.flink.autoscaler.jdbc.state.JdbcAutoScalerStateStore;
import org.apache.flink.autoscaler.jdbc.state.JdbcStateInteractor;
import org.apache.flink.autoscaler.jdbc.state.JdbcStateStore;
import org.apache.flink.autoscaler.state.AutoScalerStateStore;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.kubernetes.operator.autoscaler.state.ConfigMapStore;
import org.apache.flink.kubernetes.operator.autoscaler.state.KubernetesAutoScalerStateStore;
import org.apache.flink.kubernetes.operator.config.KubernetesOperatorConfigOptions;
import org.apache.flink.kubernetes.operator.resources.ClusterResourceManager;
import org.apache.flink.kubernetes.operator.utils.EventRecorder;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.javaoperatorsdk.operator.processing.event.ResourceID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;

/** The factory of {@link JobAutoScaler}. */
public class AutoscalerFactory {

    private static final Logger LOG = LoggerFactory.getLogger(AutoscalerFactory.class);

    public static JobAutoScaler<ResourceID, KubernetesJobAutoScalerContext> create(
            KubernetesClient client,
            Configuration config,
            EventRecorder eventRecorder,
            ClusterResourceManager clusterResourceManager) {

        String stateStoreType = config.get(KubernetesOperatorConfigOptions.STATE_STORE_TYPE);
        String eventHandlerType = config.get(KubernetesOperatorConfigOptions.EVENT_HANDLER_TYPE);
        LOG.info("state-store.type: " + stateStoreType);
        LOG.info("event-handler.type: " + eventHandlerType);
        var stateStore =
                "JDBC".equals(stateStoreType)
                        ? createJdbcStateStore(config)
                        : new KubernetesAutoScalerStateStore(new ConfigMapStore(client));
        var eventHandler =
                "JDBC".equals(eventHandlerType)
                        ? createJdbcEventHandler(config)
                        : new KubernetesAutoScalerEventHandler(eventRecorder);

        return new JobAutoScalerImpl<>(
                new RestApiMetricsCollector<>(),
                new ScalingMetricEvaluator(),
                new ScalingExecutor<>(eventHandler, stateStore, clusterResourceManager),
                eventHandler,
                new KubernetesScalingRealizer(),
                stateStore);
    }

    private static AutoScalerEventHandler createJdbcEventHandler(Configuration config) {
        return new JdbcAutoScalerEventHandler<>(
                new JdbcEventInteractor(getJdbcConnection(config)),
                config.get(KubernetesOperatorConfigOptions.JDBC_EVENT_HANDLER_TTL));
    }

    private static AutoScalerStateStore createJdbcStateStore(Configuration config) {
        return new JdbcAutoScalerStateStore<>(
                new JdbcStateStore(new JdbcStateInteractor(getJdbcConnection(config))));
    }

    private static Connection getJdbcConnection(Configuration config) {
        final var jdbcUrl = config.get(KubernetesOperatorConfigOptions.JDBC_URL);
        var user = config.get(KubernetesOperatorConfigOptions.JDBC_USERNAME);
        var password = config.get(KubernetesOperatorConfigOptions.JDBC_PASSWORD_ENV_VARIABLE);
        HikariConfig hikariConfig = new HikariConfig();
        hikariConfig.setDriverClassName("com.mysql.jdbc.Driver");
        hikariConfig.setJdbcUrl(jdbcUrl);
        hikariConfig.setUsername(user);
        hikariConfig.setPassword(password);
        try {
            return new HikariDataSource(hikariConfig).getConnection();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}
