package org.apache.flink.kubernetes.operator.plugins.listener;

import org.apache.flink.api.common.JobStatus;
import org.apache.flink.autoscaler.event.AutoScalerEventHandler;
import org.apache.flink.autoscaler.state.AutoScalerStateStore;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.kubernetes.operator.api.FlinkDeployment;
import org.apache.flink.kubernetes.operator.api.FlinkSessionJob;
import org.apache.flink.kubernetes.operator.api.listener.FlinkResourceListener;
import org.apache.flink.kubernetes.operator.api.status.FlinkDeploymentStatus;
import org.apache.flink.kubernetes.operator.api.status.FlinkSessionJobStatus;
import org.apache.flink.kubernetes.operator.autoscaler.KubernetesJobAutoScalerContext;
import org.apache.flink.runtime.util.config.memory.JvmMetaspaceAndOverheadOptions;
import org.apache.flink.runtime.util.config.memory.ProcessMemoryOptions;
import org.apache.flink.runtime.util.config.memory.ProcessMemoryUtils;
import org.apache.flink.runtime.util.config.memory.taskmanager.TaskExecutorFlinkMemory;
import org.apache.flink.runtime.util.config.memory.taskmanager.TaskExecutorFlinkMemoryUtils;

import io.fabric8.kubernetes.api.model.Event;
import io.javaoperatorsdk.operator.processing.event.ResourceID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.Arrays;

public class BkBaseFlinkResourceListener implements FlinkResourceListener {

    private static final Logger LOG = LoggerFactory.getLogger(BkBaseFlinkResourceListener.class);

    public static final ProcessMemoryUtils<TaskExecutorFlinkMemory> FLINK_MEMORY_UTILS =
            new ProcessMemoryUtils<>(getMemoryOptions(), new TaskExecutorFlinkMemoryUtils());

    private static AutoScalerStateStore<ResourceID, KubernetesJobAutoScalerContext>
            autoScalerStateStore;
    private static AutoScalerEventHandler<ResourceID, KubernetesJobAutoScalerContext>
            autoScalerEventHandler;

    public BkBaseFlinkResourceListener() {
        LOG.info("init BkBaseFlinkResourceListener");
        //        autoScalerStateStore = AutoScalerStateHolder.getStateStore();
        //        autoScalerEventHandler = AutoScalerStateHolder.getEventHandler();
    }

    private static ProcessMemoryOptions getMemoryOptions() {
        return new ProcessMemoryOptions(
                Arrays.asList(
                        TaskManagerOptions.TASK_HEAP_MEMORY,
                        TaskManagerOptions.MANAGED_MEMORY_SIZE),
                TaskManagerOptions.TOTAL_FLINK_MEMORY,
                TaskManagerOptions.TOTAL_PROCESS_MEMORY,
                new JvmMetaspaceAndOverheadOptions(
                        TaskManagerOptions.JVM_METASPACE,
                        TaskManagerOptions.JVM_OVERHEAD_MIN,
                        TaskManagerOptions.JVM_OVERHEAD_MAX,
                        TaskManagerOptions.JVM_OVERHEAD_FRACTION));
    }

    private void printlnFlinkDeploymentJobStatus(
            String tag, FlinkDeployment flinkResource, FlinkDeploymentStatus jobStatus) {
        String namespace = flinkResource.getMetadata().getNamespace();
        String jobName = flinkResource.getMetadata().getName(); // NAME
        JobStatus state = jobStatus.getJobStatus().getState(); // JOB STATUS
        String lifecycleState = jobStatus.getLifecycleState().name(); // LIFECYCLE STATE
        LOG.info(
                "TAG: {}, NAMESPACE: {}, NAME: {}, JOB STATUS: {}, LIFECYCLE STATE: {}",
                tag,
                namespace,
                jobName,
                state,
                lifecycleState);
    }

    @Override
    public void onDeploymentStatusUpdate(
            StatusUpdateContext<FlinkDeployment, FlinkDeploymentStatus> statusUpdateContext) {
        FlinkDeployment flinkResource = statusUpdateContext.getFlinkResource();
        FlinkDeploymentStatus previousStatus = statusUpdateContext.getPreviousStatus();
        FlinkDeploymentStatus newStatus = statusUpdateContext.getNewStatus();
        Instant timestamp = statusUpdateContext.getTimestamp();
        printlnFlinkDeploymentJobStatus(
                "FlinkDeployment previousStatus", flinkResource, previousStatus);
        printlnFlinkDeploymentJobStatus("FlinkDeployment newStatus", flinkResource, newStatus);
    }

    @Override
    public void onDeploymentEvent(ResourceEventContext<FlinkDeployment> ctx) {
        Event e = ctx.getEvent();
        if (e.getReason().equals("MemoryPressure")) {
            handleMemoryPressureEvent(ctx, e);
        }
    }

    private void printlnFlinkSessionJobStatus(
            String tag, FlinkSessionJob flinkResource, FlinkSessionJobStatus jobStatus) {
        String namespace = flinkResource.getMetadata().getNamespace();
        String jobName = flinkResource.getMetadata().getName(); // NAME
        JobStatus state = jobStatus.getJobStatus().getState(); // JOB STATUS
        String lifecycleState = jobStatus.getLifecycleState().name(); // LIFECYCLE STATE
        LOG.info(
                "TAG: {}, NAMESPACE: {}, NAME: {}, JOB STATUS: {}, LIFECYCLE STATE: {}",
                tag,
                namespace,
                jobName,
                state,
                lifecycleState);
    }

    @Override
    public void onSessionJobStatusUpdate(
            StatusUpdateContext<FlinkSessionJob, FlinkSessionJobStatus> statusUpdateContext) {
        FlinkSessionJob flinkResource = statusUpdateContext.getFlinkResource();
        FlinkSessionJobStatus previousStatus = statusUpdateContext.getPreviousStatus();
        FlinkSessionJobStatus newStatus = statusUpdateContext.getNewStatus();

        printlnFlinkSessionJobStatus(
                "FlinkSessionJob previousStatus", flinkResource, previousStatus);
    }

    @Override
    public void onSessionJobEvent(ResourceEventContext<FlinkSessionJob> resourceEventContext) {}

    @Override
    public void onStateSnapshotEvent(FlinkStateSnapshotEventContext ctx) {}

    @Override
    public void onStateSnapshotStatusUpdate(FlinkStateSnapshotStatusUpdateContext ctx) {}

    private void handleMemoryPressureEvent(
            ResourceEventContext<FlinkDeployment> context, Event event) {
        // TODO
    }
}
