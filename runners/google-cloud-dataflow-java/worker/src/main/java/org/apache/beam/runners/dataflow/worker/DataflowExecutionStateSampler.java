package org.apache.beam.runners.dataflow.worker;

import java.util.HashSet;
import java.util.Set;
import org.apache.beam.runners.core.metrics.ExecutionStateSampler;
import org.apache.beam.runners.core.metrics.ExecutionStateTracker;
import org.apache.beam.runners.dataflow.worker.DataflowExecutionContext.DataflowExecutionStateTracker;
import org.joda.time.DateTimeUtils.MillisProvider;

public class DataflowExecutionStateSampler extends ExecutionStateSampler {

  // private static final Logger LOG = LoggerFactory.getLogger(DataflowExecutionStateSampler.class);

  // protected Map<Long, Map<String, Set<Tuple>>> removedProcessingTimesPerKey = new HashMap<>();

  // public Map<String, Set<Tuple>> getRemovedProcessingTimersPerKey(Long workToken) {
  //   return this.removedProcessingTimesPerKey.getOrDefault(workToken,
  //       new HashMap<String, Set<Tuple>>());
  // }


  private static final MillisProvider SYSTEM_MILLIS_PROVIDER = System::currentTimeMillis;

  private static final DataflowExecutionStateSampler INSTANCE =
      new DataflowExecutionStateSampler(SYSTEM_MILLIS_PROVIDER);

  public static DataflowExecutionStateSampler instance() {
    return INSTANCE;
  }

  public DataflowExecutionStateSampler(MillisProvider clock) {
    super(clock);
  }

  @Override
  public void removeTracker(ExecutionStateTracker tracker) {
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
}