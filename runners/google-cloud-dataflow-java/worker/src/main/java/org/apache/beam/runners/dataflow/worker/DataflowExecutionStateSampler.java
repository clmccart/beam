package org.apache.beam.runners.dataflow.worker;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.beam.runners.core.metrics.ExecutionStateSampler;
import org.apache.beam.runners.core.metrics.ExecutionStateTracker;
import org.apache.beam.runners.dataflow.worker.DataflowExecutionContext.DataflowExecutionStateTracker;
import org.apache.beam.runners.dataflow.worker.DataflowExecutionContext.Tuple;
import org.joda.time.DateTimeUtils.MillisProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DataflowExecutionStateSampler extends ExecutionStateSampler {

  private static final Logger LOG = LoggerFactory.getLogger(DataflowExecutionStateSampler.class);

  protected Map<Long, Map<String, Tuple>> removedProcessingTimesPerKey = new HashMap<>();

  public Map<String, Tuple> getRemovedProcessingTimersPerKey(Long workToken) {
    return this.removedProcessingTimesPerKey.getOrDefault(workToken, new HashMap<>());
  }


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

    DataflowExecutionStateTracker dfTracker = (DataflowExecutionStateTracker) tracker;
    removedProcessingTimesPerKey.put(dfTracker.getWorkToken(), dfTracker.getStepToProcessingTime());
    LOG.info("CLAIRE TEST");
    // if (dfTracker.getStartToFinishProcessingTimeInMillis() > 0) {
    //   LOG.info("CLAIRE TEST addingWorkToken {} to removedMap for step {}", dfTracker.getWorkToken(),
    //       dfTracker.stepName);
    //   addToRemovedProcessingTimersPerKey(
    //       dfTracker.getWorkToken(), dfTracker.stepName,
    //       dfTracker.getStartToFinishProcessingTimeInMillis());
    //   for (Entry<String, Long> finishedOnTracker : dfTracker.getStepToProcessingTime().entrySet()) {
    //     addToRemovedProcessingTimersPerKey(dfTracker.getWorkToken(), finishedOnTracker.getKey(),
    //         finishedOnTracker.getValue());
    //   }
    // }

    // DataflowExecutionStateTracker dfTracker = (DataflowExecutionStateTracker) tracker;
    // // Is the tracker here in state active?
    // LOG.info("CLAIRE TEST dfTracker state during removal: {}",
    //     dfTracker.getCurrentState().getStateName());

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
}
