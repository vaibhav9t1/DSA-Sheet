QueueWorker:


package com.vonage.resischeduler.job;

import com.vonage.resischeduler.config.job.JobDefinition;
import com.vonage.resischeduler.entity.job.JobInstance;
import com.vonage.resischeduler.enums.JobInstanceStatus;
import com.vonage.resischeduler.repository.job.JobInstanceRepository;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;

/**
 * Worker that wakes up periodically and delegates IN_PROGRESS processing to QueueProcessingJob. The
 * worker cancels itself by calling queueProcessingJob.cancelWorker(...) when the job reaches a
 * terminal state.
 */
@Slf4j
public class QueueWorker implements Runnable {

  private final Long jobInstanceId;
  private final JobDefinition jobDefinition;
  private final QueueProcessingJob queueProcessingJob;
  private final JobInstanceRepository jobInstanceRepository;

  /** Constructor Class. */
  public QueueWorker(
      final Long jobInstanceId,
      final JobDefinition jobDefinition,
      final QueueProcessingJob queueProcessingJob,
      final JobInstanceRepository jobInstanceRepository) {
    this.jobInstanceId = jobInstanceId;
    this.jobDefinition = jobDefinition;
    this.queueProcessingJob = queueProcessingJob;
    this.jobInstanceRepository = jobInstanceRepository;
  }

  /** Main Run Method. */
  @Override
  public void run() {
    try {
      Optional<JobInstance> maybe = jobInstanceRepository.findById(jobInstanceId);
      if (maybe.isEmpty()) {
        log.warn("QueueWorker: jobInstance {} not found — cancelling worker", jobInstanceId);
        queueProcessingJob.cancelWorker(jobInstanceId);
        return;
      }

      JobInstance jobInstance = maybe.get();
      if (jobInstance.getStatus() == JobInstanceStatus.COMPLETED
          || jobInstance.getStatus() == JobInstanceStatus.FAILED) {
        log.info(
            "QueueWorker: jobInstance {} in terminal state {}, cancelling worker",
            jobInstanceId,
            jobInstance.getStatus());
        queueProcessingJob.cancelWorker(jobInstanceId);
        return;
      }

      queueProcessingJob.processJobInstance(jobInstanceId, jobDefinition);

      Optional<JobInstance> refreshed = jobInstanceRepository.findById(jobInstanceId);
      if (refreshed.isPresent()) {
        JobInstance after = refreshed.get();
        if (isTerminalState(after.getStatus())) {
          log.info(
              "QueueWorker: jobInstance {} reached terminal state {}, cancelling worker",
              jobInstanceId,
              after.getStatus());
          queueProcessingJob.cancelWorker(jobInstanceId);
        }
      }
    } catch (Exception ex) {
      log.error(
          "QueueWorker encountered an error for jobInstance {}: {}",
          jobInstanceId,
          ex.getMessage(),
          ex);
    }
  }

  /** Check if the job status is terminal (COMPLETED or FAILED). */
  private boolean isTerminalState(JobInstanceStatus status) {
    return status == JobInstanceStatus.COMPLETED || status == JobInstanceStatus.FAILED;
  }
}
