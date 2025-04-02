package org.apache.flink.kubernetes.operator.plugins.listener;

import org.apache.flink.api.common.JobStatus;
import org.apache.flink.kubernetes.operator.api.FlinkDeployment;
import org.apache.flink.kubernetes.operator.api.FlinkSessionJob;
import org.apache.flink.kubernetes.operator.api.listener.FlinkResourceListener;
import org.apache.flink.kubernetes.operator.api.status.FlinkDeploymentStatus;
import org.apache.flink.kubernetes.operator.api.status.FlinkSessionJobStatus;
import org.apache.flink.kubernetes.operator.utils.JobManagerIpCache;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;

public class BkBaseFlinkResourceListener implements FlinkResourceListener {

    private static final Logger LOG = LoggerFactory.getLogger(BkBaseFlinkResourceListener.class);

    public BkBaseFlinkResourceListener() {}

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
        if (!previousStatus.getJobStatus().getState().equals(JobStatus.RUNNING)
                && newStatus.getJobStatus().getState().equals(JobStatus.RUNNING)) {
            JobManagerIpCache.refresh(
                    flinkResource.getMetadata().getName(),
                    flinkResource.getMetadata().getNamespace());
        }
    }

    @Override
    public void onDeploymentEvent(ResourceEventContext<FlinkDeployment> ctx) {}

    @Override
    public void onSessionJobStatusUpdate(
            StatusUpdateContext<FlinkSessionJob, FlinkSessionJobStatus> statusUpdateContext) {}

    @Override
    public void onSessionJobEvent(ResourceEventContext<FlinkSessionJob> resourceEventContext) {}

    @Override
    public void onStateSnapshotEvent(FlinkStateSnapshotEventContext ctx) {}

    @Override
    public void onStateSnapshotStatusUpdate(FlinkStateSnapshotStatusUpdateContext ctx) {}
}
