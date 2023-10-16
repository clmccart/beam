package org.apache.beam.runners.dataflow.worker;

import org.apache.beam.runners.core.metrics.ExecutionStateSampler;
import org.apache.beam.runners.core.metrics.ExecutionStateTracker;
import org.joda.time.DateTimeUtils.MillisProvider;

public class DataflowExecutionStateSampler extends ExecutionStateSampler {

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
    super.removeTracker(tracker);
  }

  @Override
  public void doSampling(long millisSinceLastSample) {
    super.doSampling(millisSinceLastSample);
  }

}
