package org.apache.beam.examples;

import static com.google.common.base.Preconditions.checkArgument;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.DoFn;
import java.util.concurrent.ThreadLocalRandom;
import javax.annotation.Nullable;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.DynamicDestinations;
import org.apache.beam.sdk.io.gcp.bigquery.TableDestination;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.StreamingOptions;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.ValueInSingleWindow;
import org.joda.time.Duration;

/**
 * Streaming Dataflow Pipeline that reads from PubSub and inserts into BigQuery
 *
 * <p>See go/streaming-loadtest for more details.
 */
final class UserDoFnTrackingLoadTestPipeline {

  private UserDoFnTrackingLoadTestPipeline() {
  }

  private static int getDynamicTableDestIdx(List<Integer> loadDistribution) {
    // distribution will be weighted as how much this table weights / sum of all table weights
    int sum = loadDistribution.stream().reduce(0, Integer::sum);
    int randNum = ThreadLocalRandom.current().nextInt(sum);
    int accumativePercentage = 0;
    int idx = 0;
    for (Integer percentage : loadDistribution) {
      accumativePercentage += percentage;
      if (randNum < accumativePercentage) {
        return idx;
      }
      idx += 1;
    }
    return loadDistribution.size() - 1;
  }

  private static BigQueryIO.Write<PubsubMessage> toSingleTableDestination(
      BigQueryIO.Write<PubsubMessage> writeTransform, Options options) {
    return writeTransform
        .to(
            new TableReference()
                .setProjectId("cloud-millwheel")
                .setDatasetId(options.getDatasetId())
                .setTableId(options.getOutputTable()))
        .withSchema(
            new TableSchema()
                .setFields(
                    new ArrayList<>(
                        ImmutableList.of(
                            new TableFieldSchema().setName("id").setType("STRING"),
                            new TableFieldSchema().setName("description").setType("STRING"),
                            new TableFieldSchema().setName("value").setType("INTEGER"),
                            new TableFieldSchema().setName("test_string").setType("STRING")))));
  }

  private static BigQueryIO.Write<PubsubMessage> toMultipleDynamicTableDestinations(
      BigQueryIO.Write<PubsubMessage> writeTransform, Options options) {
    List<Integer> distribution = options.getDestLoadDistribution();
    String project = "cloud-millwheel";
    String dataset = options.getDatasetId();
    String outputTable = options.getOutputTable();
    return writeTransform.to(
        new DynamicDestinations<PubsubMessage, Integer>() {
          public Integer getDestination(ValueInSingleWindow<PubsubMessage> input) {
            return getDynamicTableDestIdx(distribution);
          }

          public TableDestination getTable(Integer groupIdx) {
            return new TableDestination(
                new TableReference()
                    .setProjectId(project)
                    .setDatasetId(dataset)
                    .setTableId(outputTable + "_" + groupIdx + "_of_" + (distribution.size() - 1)),
                /*tableDescription=*/ "");
          }

          public TableSchema getSchema(Integer groupIdx) {
            return new TableSchema()
                .setFields(
                    new ArrayList<>(
                        ImmutableList.of(
                            new TableFieldSchema().setName("id").setType("STRING"),
                            new TableFieldSchema().setName("description").setType("STRING"),
                            new TableFieldSchema().setName("value").setType("INTEGER"),
                            new TableFieldSchema().setName("test_string").setType("STRING"))));
          }
        });
  }

  static BigQueryIO.Write<PubsubMessage> expandStorageApi(
      Options options, BigQueryIO.Write<PubsubMessage> writeTransform) {
    // Exactly-once StorageApi
    writeTransform =
        writeTransform
            .withMethod(BigQueryIO.Write.Method.STORAGE_WRITE_API)
            .withTriggeringFrequency(
                org.joda.time.Duration.standardSeconds(
                    options.getStorageApiTriggeringFrequencySecs()));
    // TODO(b/243712610): Need to pass in --experiments=enable_streaming_auto_sharding=true to
    // pass DFE check to allow autosharding as well. Remove this comment once the check has been
    // removed.
    if (options.getEnableAutoSharding()) {
      writeTransform = writeTransform.withAutoSharding();
    } else {
      writeTransform =
          writeTransform.withNumStorageWriteApiStreams(options.getStorageApiNumStreams());
    }
    return writeTransform;
  }

  static BigQueryIO.Write<PubsubMessage> expandStreamingInserts(
      Options options, BigQueryIO.Write<PubsubMessage> writeTransform) {
    // Streaming Inserts
    writeTransform = writeTransform.withMethod(BigQueryIO.Write.Method.STREAMING_INSERTS);
    if (options.getEnableAutoSharding()) {
      checkArgument(!options.getSkipReshuffle(), "Not supported with autosharding");
      writeTransform = writeTransform.withAutoSharding();
    } else if (options.getSkipReshuffle()) {
      String messageIdAttribute = options.getMessageIdAttribute();
      checkArgument(
          !Strings.isNullOrEmpty(messageIdAttribute), "Must specify a message id attribute");
      checkArgument(!options.getIgnoreInsertIds(), "The custom Id would be ignored");
      writeTransform =
          writeTransform.withDeterministicRecordIdFn(
              (SerializableFunction<PubsubMessage, String>)
                  message1 -> message1.getAttribute(messageIdAttribute));
    }
    if (options.getIgnoreInsertIds()) {
      writeTransform = writeTransform.ignoreInsertIds();
    }
    return writeTransform;
  }

  static void expandBq(Options options, PCollection<PubsubMessage> input) {
    String messageIdAttribute = options.getMessageIdAttribute();
    BigQueryIO.Write<PubsubMessage> writeTransform =
        BigQueryIO.<PubsubMessage>write()
            .withFormatFunction(
                (SerializableFunction<PubsubMessage, TableRow>)
                    input1 ->
                        new TableRow()
                            .set("id", input1.getAttribute(messageIdAttribute))
                            .set("description", "load-test")
                            .set("value", 1)
                            .set(
                                "test_string",
                                new String(input1.getPayload(), StandardCharsets.UTF_8)))
            .withCreateDisposition(CreateDisposition.CREATE_IF_NEEDED)
            .withWriteDisposition(WriteDisposition.WRITE_APPEND);

    writeTransform = expandStreamingInserts(options, writeTransform);

    if (options.getDestLoadDistribution() == null
        || options.getDestLoadDistribution().size() == 1) {
      writeTransform = toSingleTableDestination(writeTransform, options);
    } else {
      writeTransform = toMultipleDynamicTableDestinations(writeTransform, options);
    }
    input.apply("BigQueryWriter", writeTransform);
  }

  static class AXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXDoFnWithALongNameXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX extends
      DoFn<PubsubMessage, PubsubMessage> {

    @ProcessElement
    public void processElement(@Element PubsubMessage element,
        OutputReceiver<PubsubMessage> receiver) {
      int min = 10000; // Minimum value of range
      int max = 30000; // Maximum value of range
      // Generate random int value from min to max
      int random_int = (int) Math.floor(Math.random() * (max - min + 1) + min);
      try {
        java.util.concurrent.TimeUnit.MILLISECONDS.sleep(random_int);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
      receiver.output(element);
    }
  }

  static class BXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXDoFnWithALongNameXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX extends
      DoFn<PubsubMessage, PubsubMessage> {

    @ProcessElement
    public void processElement(@Element PubsubMessage element,
        OutputReceiver<PubsubMessage> receiver) {
      int min = 10000; // Minimum value of range
      int max = 15000; // Maximum value of range
      // Generate random int value from min to max
      int random_int = (int) Math.floor(Math.random() * (max - min + 1) + min);
      try {
        java.util.concurrent.TimeUnit.MILLISECONDS.sleep(random_int);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
      receiver.output(element);
    }
  }

  static class CXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXDoFnWithALongNameXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX extends
      DoFn<PubsubMessage, PubsubMessage> {

    @ProcessElement
    public void processElement(@Element PubsubMessage element,
        OutputReceiver<PubsubMessage> receiver) {
      receiver.output(element);
    }
  }

  static class DXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXDoFnWithALongNameXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX extends
      DoFn<PubsubMessage, PubsubMessage> {

    @ProcessElement
    public void processElement(@Element PubsubMessage element,
        OutputReceiver<PubsubMessage> receiver) {
      receiver.output(element);
    }
  }

  static class EXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXDoFnWithALongNameXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX extends
      DoFn<PubsubMessage, PubsubMessage> {

    @ProcessElement
    public void processElement(@Element PubsubMessage element,
        OutputReceiver<PubsubMessage> receiver) {
      receiver.output(element);
    }
  }

  static class FXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXDoFnWithALongNameXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX extends
      DoFn<PubsubMessage, PubsubMessage> {

    @ProcessElement
    public void processElement(@Element PubsubMessage element,
        OutputReceiver<PubsubMessage> receiver) {
      receiver.output(element);
    }
  }

  static class GXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXDoFnWithALongNameXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX extends
      DoFn<PubsubMessage, PubsubMessage> {

    @ProcessElement
    public void processElement(@Element PubsubMessage element,
        OutputReceiver<PubsubMessage> receiver) {
      receiver.output(element);
    }
  }

  static class HXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXDoFnWithALongNameXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX extends
      DoFn<PubsubMessage, PubsubMessage> {

    @ProcessElement
    public void processElement(@Element PubsubMessage element,
        OutputReceiver<PubsubMessage> receiver) {
      receiver.output(element);
    }
  }

  static class IXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXDoFnWithALongNameXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX extends
      DoFn<PubsubMessage, PubsubMessage> {

    @ProcessElement
    public void processElement(@Element PubsubMessage element,
        OutputReceiver<PubsubMessage> receiver) {
      receiver.output(element);
    }
  }

  static class JXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXDoFnWithALongNameXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX extends
      DoFn<PubsubMessage, PubsubMessage> {

    @ProcessElement
    public void processElement(@Element PubsubMessage element,
        OutputReceiver<PubsubMessage> receiver) {
      receiver.output(element);
    }
  }

  static class KXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXDoFnWithALongNameXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX extends
      DoFn<PubsubMessage, PubsubMessage> {

    @ProcessElement
    public void processElement(@Element PubsubMessage element,
        OutputReceiver<PubsubMessage> receiver) {
      receiver.output(element);
    }
  }

  static class LXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXDoFnWithALongNameXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX extends
      DoFn<PubsubMessage, PubsubMessage> {

    @ProcessElement
    public void processElement(@Element PubsubMessage element,
        OutputReceiver<PubsubMessage> receiver) {
      receiver.output(element);
    }
  }

  static class MXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXDoFnWithALongNameXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX extends
      DoFn<PubsubMessage, PubsubMessage> {

    @ProcessElement
    public void processElement(@Element PubsubMessage element,
        OutputReceiver<PubsubMessage> receiver) {
      receiver.output(element);
    }
  }

  static class NXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXDoFnWithALongNameXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX extends
      DoFn<PubsubMessage, PubsubMessage> {

    @ProcessElement
    public void processElement(@Element PubsubMessage element,
        OutputReceiver<PubsubMessage> receiver) {
      receiver.output(element);
    }
  }

  static class OXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXDoFnWithALongNameXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX extends
      DoFn<PubsubMessage, PubsubMessage> {

    @ProcessElement
    public void processElement(@Element PubsubMessage element,
        OutputReceiver<PubsubMessage> receiver) {
      receiver.output(element);
    }
  }

  static class PXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXDoFnWithALongNameXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX extends
      DoFn<PubsubMessage, PubsubMessage> {

    @ProcessElement
    public void processElement(@Element PubsubMessage element,
        OutputReceiver<PubsubMessage> receiver) {
      receiver.output(element);
    }
  }

  static class QXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXDoFnWithALongNameXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX extends
      DoFn<PubsubMessage, PubsubMessage> {

    @ProcessElement
    public void processElement(@Element PubsubMessage element,
        OutputReceiver<PubsubMessage> receiver) {
      receiver.output(element);
    }
  }

  static class RXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXDoFnWithALongNameXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX extends
      DoFn<PubsubMessage, PubsubMessage> {

    @ProcessElement
    public void processElement(@Element PubsubMessage element,
        OutputReceiver<PubsubMessage> receiver) {
      receiver.output(element);
    }
  }

  static class SXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXDoFnWithALongNameXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX extends
      DoFn<PubsubMessage, PubsubMessage> {

    @ProcessElement
    public void processElement(@Element PubsubMessage element,
        OutputReceiver<PubsubMessage> receiver) {
      receiver.output(element);
    }
  }

  static class TXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXDoFnWithALongNameXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX extends
      DoFn<PubsubMessage, PubsubMessage> {

    @ProcessElement
    public void processElement(@Element PubsubMessage element,
        OutputReceiver<PubsubMessage> receiver) {
      receiver.output(element);
    }
  }

  static class UXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXDoFnWithALongNameXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX extends
      DoFn<PubsubMessage, PubsubMessage> {

    @ProcessElement
    public void processElement(@Element PubsubMessage element,
        OutputReceiver<PubsubMessage> receiver) {
      receiver.output(element);
    }
  }

  static class VXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXDoFnWithALongNameXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX extends
      DoFn<PubsubMessage, PubsubMessage> {

    @ProcessElement
    public void processElement(@Element PubsubMessage element,
        OutputReceiver<PubsubMessage> receiver) {
      receiver.output(element);
    }
  }

  static class WXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXDoFnWithALongNameXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX extends
      DoFn<PubsubMessage, PubsubMessage> {

    @ProcessElement
    public void processElement(@Element PubsubMessage element,
        OutputReceiver<PubsubMessage> receiver) {
      receiver.output(element);
    }
  }

  static class XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXDoFnWithALongNameXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX extends
      DoFn<PubsubMessage, PubsubMessage> {

    @ProcessElement
    public void processElement(@Element PubsubMessage element,
        OutputReceiver<PubsubMessage> receiver) {
      receiver.output(element);
    }
  }

  static class YXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXDoFnWithALongNameXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX extends
      DoFn<PubsubMessage, PubsubMessage> {

    @ProcessElement
    public void processElement(@Element PubsubMessage element,
        OutputReceiver<PubsubMessage> receiver) {
      receiver.output(element);
    }
  }

  static class ZXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXDoFnWithALongNameXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX extends
      DoFn<PubsubMessage, PubsubMessage> {

    @ProcessElement
    public void processElement(@Element PubsubMessage element,
        OutputReceiver<PubsubMessage> receiver) {
      receiver.output(element);
    }
  }

  static void runPipeline(Options options) {
    // Run processing pipeline
    if (Strings.isNullOrEmpty(options.getJobName())) {
      options.setJobName("nokill-clairemccarthy-" + System.currentTimeMillis());
    }

    PubsubIO.Read<PubsubMessage> read = PubsubIO.readMessagesWithAttributes();
    read =
        Strings.isNullOrEmpty(options.getMessageIdAttribute())
            ? read
            : read.withIdAttribute(options.getMessageIdAttribute());
    read =
        Strings.isNullOrEmpty(options.getTimestampAttribute())
            ? read
            : read.withTimestampAttribute(options.getTimestampAttribute());
    read =
        options.getInputSubscription() == null || options.getInputSubscription().isEmpty()
            ? read.fromTopic(options.getInputTopic())
            : read.fromSubscription(options.getInputSubscription());

    Pipeline pipeline = Pipeline.create(options);

    // TODO(clairemccarthy): add dofns here.
    PCollection<PubsubMessage> input = pipeline.apply(read);
    PCollection<PubsubMessage> inputTwo = input
        .apply(ParDo.of(
            new AXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXDoFnWithALongNameXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX()))
        .apply(ParDo.of(
            new BXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXDoFnWithALongNameXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX()))
        .apply(ParDo.of(
            new CXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXDoFnWithALongNameXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX()));
    // .apply(ParDo.of(
    //     new DXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXDoFnWithALongNameXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX()))
    // .apply(ParDo.of(
    //     new EXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXDoFnWithALongNameXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX()))
    // .apply(ParDo.of(
    //     new FXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXDoFnWithALongNameXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX()))
    // .apply(ParDo.of(
    //     new GXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXDoFnWithALongNameXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX()))
    // .apply(ParDo.of(
    //     new HXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXDoFnWithALongNameXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX()))
    // .apply(ParDo.of(
    //     new IXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXDoFnWithALongNameXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX()))
    // .apply(ParDo.of(
    //     new JXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXDoFnWithALongNameXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX()))
    // .apply(ParDo.of(
    //     new KXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXDoFnWithALongNameXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX()))
    // .apply(ParDo.of(
    //     new LXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXDoFnWithALongNameXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX()))
    // .apply(ParDo.of(
    //     new MXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXDoFnWithALongNameXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX()))
    // .apply(ParDo.of(
    //     new NXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXDoFnWithALongNameXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX()))
    // .apply(ParDo.of(
    //     new OXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXDoFnWithALongNameXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX()))
    // .apply(ParDo.of(
    //     new PXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXDoFnWithALongNameXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX()))
    // .apply(ParDo.of(
    //     new QXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXDoFnWithALongNameXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX()))
    // .apply(ParDo.of(
    //     new RXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXDoFnWithALongNameXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX()))
        // .apply(ParDo.of(
        //     new SXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXDoFnWithALongNameXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX()))
    // .apply(ParDo.of(
    //     new TXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXDoFnWithALongNameXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX()))
    // .apply(ParDo.of(
    //     new UXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXDoFnWithALongNameXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX()))
    // .apply(ParDo.of(
    //     new VXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXDoFnWithALongNameXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX()))
    // .apply(ParDo.of(
    //     new WXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXDoFnWithALongNameXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX()))
    // .apply(ParDo.of(
    //     new XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXDoFnWithALongNameXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX()))
    // .apply(ParDo.of(
    //     new YXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXDoFnWithALongNameXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX()))
    // .apply(ParDo.of(
    //     new ZXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXDoFnWithALongNameXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX()));

    if (options.getEnableBQ()) {
      expandBq(options, inputTwo);
    }
    pipeline.run();
  }

  public static interface Options extends PipelineOptions {

    @Description("Input Pubsub topic")
    String getInputTopic();

    void setInputTopic(String value);

    @Description("Input Pubsub subscription")
    String getInputSubscription();

    void setInputSubscription(String value);

    @Description("Message ID attribute")
    String getMessageIdAttribute();

    void setMessageIdAttribute(String value);

    @Description("Timestamp attribute")
    String getTimestampAttribute();

    void setTimestampAttribute(String value);

    @Description("BigQuery dataset id")
    @Default.String("bq_load_test")
    String getDatasetId();

    void setDatasetId(String value);

    @Description("Output table id. If writing to multiple tables, this is the table id prefix.")
    @Default.String("bq_inserts")
    String getOutputTable();

    void setOutputTable(String value);

    @Default.Boolean(false)
    Boolean getEnableAutoSharding();

    void setEnableAutoSharding(Boolean value);

    @Description("Ignores insert Ids when writing to BigQuery to use Vortex.")
    @Default.Boolean(false)
    Boolean getIgnoreInsertIds();

    void setIgnoreInsertIds(Boolean value);

    @Default.Boolean(false)
    Boolean getSkipReshuffle();

    void setSkipReshuffle(Boolean value);

    @Description(
        "Number of file shards per destination if using FILE_LOADS method (--useFileLoads). Only"
            + " applicable when auto-sharding is disabled.")
    @Default.Integer(0)
    Integer getNumFileShards();

    void setNumFileShards(Integer value);

    @Description("Triggering frequency if using FILE_LOADS method (--useFileLoads).")
    @Default.Integer(300)
    Integer getFileTriggeringFrequencySecs();

    void setFileTriggeringFrequencySecs(Integer value);

    @Description("Triggering frequency if using STORAGE_WRITES_API")
    @Default.Integer(5)
    Integer getStorageApiTriggeringFrequencySecs();

    void setStorageApiTriggeringFrequencySecs(Integer value);

    @Description("Number of streams when using STORAGE_WRITES_API")
    @Default.Integer(1000)
    Integer getStorageApiNumStreams();

    void setStorageApiNumStreams(Integer value);


    @Default.Boolean(true)
    Boolean getEnableBQ();

    void setEnableBQ(Boolean value);

    @Description(
        "Distribution of the streaming insertion load represented by a list of percentage of input"
            + " numbers written to each table destination. If left unspecified, all the input will"
            + " be written to a single table.")
    @Nullable
    List<Integer> getDestLoadDistribution();

    void setDestLoadDistribution(@Nullable List<Integer> value);
  }

  public static void main(String[] args) {
    Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
    options.as(StreamingOptions.class).setStreaming(true);
    runPipeline(options);
  }
}