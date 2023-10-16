package org.apache.beam.runners.dataflow.worker;

import java.util.HashMap;
import java.util.IntSummaryStatistics;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.beam.runners.core.metrics.ExecutionStateSampler;
import org.apache.beam.runners.core.metrics.ExecutionStateTracker;
import org.apache.beam.runners.dataflow.worker.DataflowExecutionContext.DataflowExecutionStateTracker;
import org.joda.time.DateTimeUtils.MillisProvider;

public class DataflowExecutionStateSampler extends ExecutionStateSampler {

  private static final MillisProvider SYSTEM_MILLIS_PROVIDER = System::currentTimeMillis;
  private static final DataflowExecutionStateSampler INSTANCE =
      new DataflowExecutionStateSampler(SYSTEM_MILLIS_PROVIDER);

  private Map<String, Map<String, IntSummaryStatistics>> completedProcessingMetrics = new HashMap<>();
  private Map<String, DataflowExecutionStateTracker> activeTrackersByWorkId = new HashMap<>();

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
  public synchronized void removeTracker(ExecutionStateTracker tracker) {
    if (tracker instanceof DataflowExecutionContext.DataflowExecutionStateTracker) {
      DataflowExecutionStateTracker dfTracker = (DataflowExecutionStateTracker) tracker;
      completedProcessingMetrics.put(dfTracker.getWorkItemId(),
          mergeStepStatsMaps(completedProcessingMetrics.getOrDefault(
                  dfTracker.getWorkItemId(), new HashMap<>()),
              dfTracker.getProcessingTimesByStep()));
      activeTrackersByWorkId.remove(dfTracker.getWorkItemId());
    }
    super.removeTracker(tracker);
  }

  private void updateTrackerMap() {
    // TODO(clairemccarthy): clear map before putting?
    for (ExecutionStateTracker tracker : activeTrackers) {
      if (!(tracker instanceof DataflowExecutionStateTracker)) {
        continue;
      }
      DataflowExecutionStateTracker dfTracker = (DataflowExecutionStateTracker) tracker;
      activeTrackersByWorkId.put(dfTracker.getWorkItemId(), dfTracker);
    }
  }

  @Override
  public void doSampling(long millisSinceLastSample) {
    updateTrackerMap();
    super.doSampling(millisSinceLastSample);
  }

  public synchronized void clearMapsForWorkId(String workId) {
    completedProcessingMetrics.remove(workId);
    activeTrackersByWorkId.remove(workId);
  }
}
