package org.apache.beam.runners.dataflow.worker;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IntSummaryStatistics;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import org.apache.beam.runners.core.metrics.ExecutionStateSampler;
import org.apache.beam.runners.core.metrics.ExecutionStateTracker;
import org.apache.beam.runners.dataflow.worker.DataflowExecutionContext.DataflowExecutionStateTracker;
import org.apache.beam.runners.dataflow.worker.DataflowExecutionContext.DataflowExecutionStateTracker.Metadata;
import org.joda.time.DateTimeUtils.MillisProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DataflowExecutionStateSampler extends ExecutionStateSampler {

  private static final Logger LOG = LoggerFactory.getLogger(DataflowExecutionStateSampler.class);

  // protected Map<Long, Map<String, Set<Tuple>>> removedProcessingTimesPerKey = new HashMap<>();

  // public Map<String, Set<Tuple>> getRemovedProcessingTimersPerKey(Long workToken) {
  //   return this.removedProcessingTimesPerKey.getOrDefault(workToken,
  //       new HashMap<String, Set<Tuple>>());
  // }


  private static final MillisProvider SYSTEM_MILLIS_PROVIDER = System::currentTimeMillis;

  private static final DataflowExecutionStateSampler INSTANCE =
      new DataflowExecutionStateSampler(SYSTEM_MILLIS_PROVIDER);

  private final Map<String, Map<String, IntSummaryStatistics>> removedProcessingMetrics = new HashMap<>();
  private final Map<String, DataflowExecutionStateTracker> trackersPerWorkId = new HashMap<>();


  public static DataflowExecutionStateSampler instance() {
    return INSTANCE;
  }

  public DataflowExecutionStateSampler(MillisProvider clock) {
    super(clock);
  }

  @Override
  public void removeTracker(ExecutionStateTracker tracker) {
    // TODO: when removing, add processing times.
    DataflowExecutionStateTracker dfTracker = (DataflowExecutionStateTracker) tracker;
    LOG.info("CLAIRE TEST {} removing tracker {} {}",
        Thread.currentThread().getId(), dfTracker.getWorkItemId(),
        dfTracker.getProcessingTimesPerStep());
    // TODO: need to handle situation where there is an active message still.
    synchronized (removedProcessingMetrics) {
      Map<String, IntSummaryStatistics> summaryStats = removedProcessingMetrics.getOrDefault(
          dfTracker.getWorkItemId(), new HashMap<>());
      for (Entry<String, IntSummaryStatistics> steps : dfTracker.getProcessingTimesPerStep()
          .entrySet()) {
        summaryStats.compute(steps.getKey(), (k, v) -> {
          if (v == null) {
            return steps.getValue();
          }
          v.combine(steps.getValue());
          return v;
        });
      }
      removedProcessingMetrics.put(dfTracker.getWorkItemId(), summaryStats);
      LOG.info("CLAIRE TEST {} removedProcessingMetrics: {}", Thread.currentThread().getId(),
          removedProcessingMetrics);
    }
    activeTrackers.remove(tracker);

    // DataflowExecutionStateTracker dfTracker = (DataflowExecutionStateTracker) tracker;
    // removedProcessingTimesPerKey.put(dfTracker.getWorkToken(),
    //     dfTracker.getStepToProcessingTimes());
    // Attribute any remaining time since the last sampling while removing the tracker.
    //
    // There is a race condition here; if sampling happens in the time between when we remove the
    // tracker from activeTrackers and read the lastSampleTicks value, the sampling time will
    // be lost for the tracker being removed. This is acceptable as sampling is already an
    // approximation of actual execution time.
    long millisSinceLastSample = clock.getMillis() - this.lastSampleTimeMillis;
    if (millisSinceLastSample > 0) {
      tracker.takeSample(millisSinceLastSample);
    }
  }

  public Map<String, IntSummaryStatistics> getProcessingDistributionsForWorkId(
      String workId) {
    if (trackersPerWorkId.containsKey(workId)) {
      DataflowExecutionStateTracker tracker = trackersPerWorkId.get(workId);
      return tracker.getProcessingTimesPerStep();
    }
    // TODO: consider making this return an optional
    return new HashMap<>();
  }

  public void clearMapsForWorkId(String workId) {
    synchronized (trackersPerWorkId) {
      trackersPerWorkId.remove(workId);
    }
    synchronized (removedProcessingMetrics) {
      removedProcessingMetrics.remove(workId);
    }
  }

  public Metadata getActiveMessageMetadataForWorkId(String workId) {
    if (trackersPerWorkId.containsKey(workId)) {
      DataflowExecutionStateTracker tracker = trackersPerWorkId.get(workId);
      return tracker.getActiveMessageMetadata();
    }
    // TODO: consider making this return an optional
    return new Metadata("", 0L);
  }

  public Set<DataflowExecutionStateTracker> getActiveTrackersForWorkId(String id) {
    Set<DataflowExecutionStateTracker> trackersForWorkId = new HashSet<>();
    // TODO: move as much computation into dosampling? same with at the tracker level.
    for (ExecutionStateTracker tracker : activeTrackers) {
      DataflowExecutionStateTracker dfTracker = (DataflowExecutionStateTracker) tracker;
      if (dfTracker.getWorkItemId().equals(id)) {
        trackersForWorkId.add(dfTracker);
      }
    }
    return trackersForWorkId;
  }

  @Override
  public void doSampling(long millisSinceLastSample) {
    updateTrackerMonitoringMap();
    super.doSampling(millisSinceLastSample);
  }

  private void updateTrackerMonitoringMap() {
    for (ExecutionStateTracker tracker : activeTrackers) {
      DataflowExecutionStateTracker dfTracker = (DataflowExecutionStateTracker) tracker;
      // TODO: i think this will result in duplicating trackers?
      trackersPerWorkId.put(dfTracker.getWorkItemId(), dfTracker);
    }
  }
}