package org.apache.beam.runners.dataflow.worker;

import java.util.HashMap;
import java.util.IntSummaryStatistics;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import javax.annotation.Nullable;
import org.apache.beam.runners.core.metrics.ExecutionStateSampler;
import org.apache.beam.runners.core.metrics.ExecutionStateTracker;
import org.apache.beam.runners.dataflow.worker.DataflowExecutionContext.DataflowExecutionStateTracker;
import org.joda.time.DateTimeUtils.MillisProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DataflowExecutionStateSampler extends ExecutionStateSampler {

  private static final MillisProvider SYSTEM_MILLIS_PROVIDER = System::currentTimeMillis;
  private static final DataflowExecutionStateSampler INSTANCE =
      new DataflowExecutionStateSampler(SYSTEM_MILLIS_PROVIDER);

  private ConcurrentHashMap<String, Map<String, IntSummaryStatistics>> completedProcessingMetrics = new ConcurrentHashMap<>();
  private ConcurrentHashMap<String, DataflowExecutionStateTracker> activeTrackersByWorkId = new ConcurrentHashMap<>();

  public static DataflowExecutionStateSampler instance() {
    return INSTANCE;
  }

  public DataflowExecutionStateSampler(MillisProvider clock) {
    super(clock);
  }

  private Map<String, IntSummaryStatistics> mergeStepStatsMaps(
      Map<String, IntSummaryStatistics> map1, Map<String, IntSummaryStatistics> map2) {
    for (Entry<String, IntSummaryStatistics> steps : map2
        .entrySet()) {
      map1.compute(steps.getKey(), (k, v) -> {
        if (v == null) {
          return steps.getValue();
        }
        v.combine(steps.getValue());
        return v;
      });
    }
    return map1;
  }

  @Override
  public void addTracker(ExecutionStateTracker tracker) {
    if (!(tracker instanceof DataflowExecutionStateTracker)) {
      return;
    }
    DataflowExecutionStateTracker dfTracker = (DataflowExecutionStateTracker) tracker;
    LOG.info("CLAIRE TEST adding DFtracker with work item id: {}", dfTracker.getWorkItemId());

    this.activeTrackersByWorkId.put(dfTracker.getWorkItemId(), dfTracker);
  }

  @Override
  public void removeTracker(ExecutionStateTracker tracker) {
    if (tracker instanceof DataflowExecutionContext.DataflowExecutionStateTracker) {
      DataflowExecutionStateTracker dfTracker = (DataflowExecutionStateTracker) tracker;
      LOG.info("CLAIRE TEST removing DFtracker with work item id: {}", dfTracker.getWorkItemId());

      // TODO(clairemccarthy): make sure active message is done before.
      completedProcessingMetrics.put(dfTracker.getWorkItemId(),
          mergeStepStatsMaps(completedProcessingMetrics.getOrDefault(
                  dfTracker.getWorkItemId(), new HashMap<>()),
              dfTracker.getProcessingTimesByStep()));
      activeTrackersByWorkId.remove(dfTracker.getWorkItemId());
    }
    super.removeTracker(tracker);
  }

  @Nullable
  public ActiveMessageMetadata getActiveMessageMetadataForWorkId(String workId) {
    LOG.info("CLAIRE TEST getting active message for work item id: {}", workId);

    if (activeTrackersByWorkId.containsKey(workId)) {
      return activeTrackersByWorkId.get(workId).getActiveMessageMetadata();
    }
    return null;
  }

  private static final Logger LOG = LoggerFactory.getLogger(DataflowExecutionStateSampler.class);

  @Nullable
  public Map<String, IntSummaryStatistics> getProcessingDistributionsForWorkId(
      String workId) {
    if (!activeTrackersByWorkId.containsKey(workId)) {
      if (completedProcessingMetrics.containsKey(workId)) {
        LOG.info("CLAIRE TEST completedProcessingMetrics contains work token {}", workId);
        Map<String, IntSummaryStatistics> completedMetrics = completedProcessingMetrics.get(workId);
        completedProcessingMetrics.remove(workId);
        return completedMetrics;
      }
      LOG.info("CLAIRE TEST completedProcessingMetrics DOES NOT contain work token {}", workId);
      return null;
    }
    DataflowExecutionStateTracker tracker = activeTrackersByWorkId.get(workId);
    return mergeStepStatsMaps(completedProcessingMetrics.getOrDefault(workId, new HashMap<>()),
        tracker.getProcessingTimesByStep());
  }

  @Override
  public void doSampling(long millisSinceLastSample) {
    for (DataflowExecutionStateTracker tracker : activeTrackersByWorkId.values()) {
      tracker.takeSample(millisSinceLastSample);
    }
  }

  public synchronized void clearMapsForWorkId(String workId) {
    completedProcessingMetrics.remove(workId);
  }
}
