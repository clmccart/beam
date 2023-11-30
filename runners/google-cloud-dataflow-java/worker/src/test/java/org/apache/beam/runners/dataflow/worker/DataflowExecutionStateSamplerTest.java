package org.apache.beam.runners.dataflow.worker;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.anEmptyMap;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.HashMap;
import java.util.IntSummaryStatistics;
import java.util.Map;
import org.apache.beam.runners.dataflow.worker.DataflowExecutionContext.DataflowExecutionStateTracker;
import org.apache.beam.runners.dataflow.worker.counters.NameContext;
import org.joda.time.DateTimeUtils.MillisProvider;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class DataflowExecutionStateSamplerTest {

  private MillisProvider clock;
  private DataflowExecutionStateSampler sampler;

  @Before
  public void setUp() {
    clock = mock(MillisProvider.class);
    sampler = DataflowExecutionStateSampler.newForTest(clock);
  }

  private final TestOperationContext.TestDataflowExecutionState step1act1 = new TestOperationContext.TestDataflowExecutionState(
      createNameContext("test-stage1"), "activity1");

  private NameContext createNameContext(String userName) {
    return NameContext.create("", "", "", userName);
  }

  @Test
  public void testAddTrackerRemoveTrackerActiveMessageMetadataGetsUpdated() {
    String workId = "work-item-id1";
    ActiveMessageMetadata testMetadata = new ActiveMessageMetadata(
        step1act1.getStepName().userName(),
        clock.getMillis());
    DataflowExecutionStateTracker trackerMock = createMockTracker(workId);
    when(trackerMock.getActiveMessageMetadata()).thenReturn(testMetadata);

    sampler.addTracker(trackerMock);
    assertThat(sampler.getActiveMessageMetadataForWorkId(workId),
        equalTo(testMetadata));

    sampler.removeTracker(trackerMock);
    Assert.assertNull(sampler.getActiveMessageMetadataForWorkId(workId));
  }

  @Test
  public void testRemoveTrackerCompletedProcessingTimesGetsUpdated() {
    String workId = "work-item-id1";
    Map<String, IntSummaryStatistics> testCompletedProcessingTimes = new HashMap<>();
    testCompletedProcessingTimes.put("some-step", new IntSummaryStatistics());
    DataflowExecutionStateTracker trackerMock = createMockTracker(workId);
    when(trackerMock.getProcessingTimesByStep()).thenReturn(testCompletedProcessingTimes);

    sampler.addTracker(trackerMock);
    sampler.removeTracker(trackerMock);

    assertThat(sampler.getProcessingDistributionsForWorkId(workId),
        equalTo(testCompletedProcessingTimes));
  }

  @Test
  public void testGetCompletedProcessingTimesAndActiveMessageFromActiveTracker() {
    String workId = "work-item-id1";
    Map<String, IntSummaryStatistics> testCompletedProcessingTimes = new HashMap<>();
    IntSummaryStatistics testSummaryStats = new IntSummaryStatistics();
    testSummaryStats.accept(1);
    testSummaryStats.accept(3);
    testSummaryStats.accept(5);
    testCompletedProcessingTimes.put("some-step", testSummaryStats);
    ActiveMessageMetadata testMetadata = new ActiveMessageMetadata(
        step1act1.getStepName().userName(),
        clock.getMillis());
    DataflowExecutionStateTracker trackerMock = createMockTracker(workId);
    when(trackerMock.getActiveMessageMetadata()).thenReturn(testMetadata);
    when(trackerMock.getProcessingTimesByStep()).thenReturn(testCompletedProcessingTimes);

    sampler.addTracker(trackerMock);

    assertThat(sampler.getActiveMessageMetadataForWorkId(workId),
        equalTo(testMetadata));
    assertThat(sampler.getProcessingDistributionsForWorkId(workId),
        equalTo(testCompletedProcessingTimes));
  }

  @Test
  public void testResetForWorkIdClearsMaps() {
    String workId1 = "work-item-id1";
    String workId2 = "work-item-id2";
    DataflowExecutionStateTracker tracker1Mock = createMockTracker(workId1);
    DataflowExecutionStateTracker tracker2Mock = createMockTracker(workId2);

    sampler.addTracker(tracker1Mock);
    sampler.addTracker(tracker2Mock);

    assertThat(sampler.getActiveMessageMetadataForWorkId(workId1),
        equalTo(tracker1Mock.getActiveMessageMetadata()));
    assertThat(sampler.getProcessingDistributionsForWorkId(workId1),
        equalTo(tracker1Mock.getProcessingTimesByStep()));
    assertThat(sampler.getActiveMessageMetadataForWorkId(workId2),
        equalTo(tracker2Mock.getActiveMessageMetadata()));
    assertThat(sampler.getProcessingDistributionsForWorkId(workId2),
        equalTo(tracker2Mock.getProcessingTimesByStep()));

    sampler.removeTracker(tracker1Mock);
    sampler.removeTracker(tracker2Mock);
    sampler.resetForWorkId(workId2);

    assertThat(sampler.getProcessingDistributionsForWorkId(workId1),
        equalTo(tracker1Mock.getProcessingTimesByStep()));
    Assert.assertNull(sampler.getProcessingDistributionsForWorkId(workId2));
  }

  private DataflowExecutionStateTracker createMockTracker(String workItemId) {
    DataflowExecutionStateTracker trackerMock = mock(DataflowExecutionStateTracker.class);
    when(trackerMock.getWorkItemId()).thenReturn(workItemId);
    return trackerMock;
  }
}