package org.apache.flink.kubernetes.operator.plugins.listener;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.autoscaler.JobAutoScalerContext;
import org.apache.flink.autoscaler.event.AutoScalerEventHandler;
import org.apache.flink.autoscaler.state.AutoScalerStateStore;
import org.apache.flink.autoscaler.tuning.ConfigChanges;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.kubernetes.operator.api.FlinkDeployment;
import org.apache.flink.kubernetes.operator.api.FlinkSessionJob;
import org.apache.flink.kubernetes.operator.api.listener.FlinkResourceListener;
import org.apache.flink.kubernetes.operator.api.spec.Resource;
import org.apache.flink.kubernetes.operator.api.status.FlinkDeploymentStatus;
import org.apache.flink.kubernetes.operator.api.status.FlinkSessionJobStatus;
import org.apache.flink.kubernetes.operator.autoscaler.AutoscalerFactory;
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

    private static AutoScalerStateStore autoScalerStateStore;
    private static AutoScalerEventHandler autoScalerEventHandler;

    public BkBaseFlinkResourceListener() {
        LOG.info("================ init BkBaseFlinkResourceListener");
        autoScalerStateStore = AutoscalerFactory.getStateStore();
        autoScalerEventHandler = AutoscalerFactory.getEventHandler();
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
                "================ TAG: {}, NAMESPACE: {}, NAME: {}, JOB STATUS: {}, LIFECYCLE STATE: {}",
                tag,
                namespace,
                jobName,
                state,
                lifecycleState);
    }

    @Override
    public void onDeploymentStatusUpdate(
            StatusUpdateContext<FlinkDeployment, FlinkDeploymentStatus> statusUpdateContext) {
        LOG.info("===================<<< onDeploymentStatusUpdate");
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
        LOG.info("===================<<< onDeploymentEvent");
        Event e = ctx.getEvent();
        LOG.info("===================<<< Event: {}", e);
        if (e.getReason().equals("MemoryPressure")) {
            LOG.info("===================<<< Event is MemoryPressure");
            LOG.info(
                    "===================<<< Event reason: {}, message: {}",
                    e.getReason(),
                    e.getMessage());
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
                "===================<<< TAG: {}, NAMESPACE: {}, NAME: {}, JOB STATUS: {}, LIFECYCLE STATE: {}",
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
    public void onSessionJobEvent(ResourceEventContext<FlinkSessionJob> resourceEventContext) {
        LOG.info("===================<<< onSessionJobEvent");
    }

    @Override
    public void onStateSnapshotEvent(FlinkStateSnapshotEventContext ctx) {
        LOG.info("===================<<< onStateSnapshotEvent");
    }

    @Override
    public void onStateSnapshotStatusUpdate(FlinkStateSnapshotStatusUpdateContext ctx) {}

    private void handleMemoryPressureEvent(
            ResourceEventContext<FlinkDeployment> context, Event event) {
        // 处理 OOM 事件的逻辑，例如发送告警或重启 TaskManager
        LOG.error("===================<<< OOM detected in TaskManager");
        // 其他处理逻辑

        FlinkDeployment flinkDeployment = context.getFlinkResource();
        // Apply config overrides
        Resource tmResource = flinkDeployment.getSpec().getTaskManager().getResource();
        var currentMemory = MemorySize.parse(tmResource.getMemory());

        JobAutoScalerContext<ResourceID> jobAutoScalerContext = getAutoScalerContext(context);

        try {
            ConfigChanges configChanges =
                    autoScalerStateStore.getConfigChanges(jobAutoScalerContext);
            LOG.info(
                    "===================<<< previous configChanges: "
                            + configChanges
                                    .getOverrides()
                                    .get(TaskManagerOptions.TOTAL_PROCESS_MEMORY.key()));
            double overrideMemory = currentMemory.getBytes() * 1.5;
            MemorySize overrideMemorySize =
                    MemorySize.parse(String.valueOf(overrideMemory), MemorySize.MemoryUnit.BYTES);

            configChanges.addOverride(
                    TaskManagerOptions.TOTAL_PROCESS_MEMORY.key(),
                    overrideMemorySize.toHumanReadableString());
            autoScalerStateStore.storeConfigChanges(jobAutoScalerContext, configChanges);
            ConfigChanges newConfigChanges =
                    autoScalerStateStore.getConfigChanges(jobAutoScalerContext);
            LOG.info(
                    "===================<<< new configChanges: "
                            + newConfigChanges
                                    .getOverrides()
                                    .get(TaskManagerOptions.TOTAL_PROCESS_MEMORY.key()));

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private JobAutoScalerContext<ResourceID> getAutoScalerContext(
            ResourceEventContext<FlinkDeployment> context) {
        FlinkDeployment flinkDeployment = context.getFlinkResource();
        org.apache.flink.kubernetes.operator.api.status.JobStatus jobStatus =
                flinkDeployment.getStatus().getJobStatus();
        return new JobAutoScalerContext<>(
                ResourceID.fromResource(flinkDeployment),
                JobID.fromHexString(jobStatus.getJobId()),
                jobStatus.getState(),
                new Configuration(),
                null,
                null);
    }
}
