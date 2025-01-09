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

import org.apache.flink.autoscaler.event.AutoScalerEventHandler;
import org.apache.flink.autoscaler.state.AutoScalerStateStore;

import io.javaoperatorsdk.operator.processing.event.ResourceID;

public class AutoScalerStateHolder {

    private static final ThreadLocal<
                    AutoScalerStateStore<ResourceID, KubernetesJobAutoScalerContext>>
            STATE_STORE_LOCAL = new ThreadLocal<>();
    private static final ThreadLocal<
                    AutoScalerEventHandler<ResourceID, KubernetesJobAutoScalerContext>>
            EVENT_HANDLER_LOCAL = new ThreadLocal<>();

    private AutoScalerStateHolder() {}

    public static void setStateStore(
            AutoScalerStateStore<ResourceID, KubernetesJobAutoScalerContext> stateStore) {
        STATE_STORE_LOCAL.set(stateStore);
    }

    public static AutoScalerStateStore<ResourceID, KubernetesJobAutoScalerContext> getStateStore() {
        return STATE_STORE_LOCAL.get();
    }

    public static void setEventHandler(
            AutoScalerEventHandler<ResourceID, KubernetesJobAutoScalerContext> eventHandler) {
        EVENT_HANDLER_LOCAL.set(eventHandler);
    }

    public static AutoScalerEventHandler<ResourceID, KubernetesJobAutoScalerContext>
            getEventHandler() {
        return EVENT_HANDLER_LOCAL.get();
    }

    public static void remove() {
        STATE_STORE_LOCAL.remove();
        EVENT_HANDLER_LOCAL.remove();
    }
}
