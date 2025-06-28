package com.google.cloud.teleport.v2.datastream.sources;

import com.google.cloud.teleport.v2.coders.FailsafeElementCoder;
import com.google.cloud.teleport.v2.datastream.transforms.FormatDatastreamRecordToJson;
import com.google.cloud.teleport.v2.values.FailsafeElement;
import java.util.HashMap;
import java.util.Map;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.extensions.avro.io.AvroSource;
import org.apache.beam.sdk.io.FileBasedSource;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.FileIO.ReadableFile;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Reshuffle;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DatastreamReader extends PTransform<PBegin, PCollection<FailsafeElement<String, String>>> {

  private static final Logger LOG = LoggerFactory.getLogger(DatastreamReader.class);
  private String inputFilePattern;
  private String streamName;
  private Boolean lowercaseSourceColumns = false;
  private Boolean hashRowId = false;
  private Map<String, String> renameColumns = new HashMap<>();
  PCollection<String> directories = null;

  public DatastreamReader(String streamName, String inputFilePattern) {
    this.streamName = streamName;
    this.inputFilePattern = inputFilePattern;
  }

  @Override
  public PCollection<FailsafeElement<String, String>> expand(PBegin input) {
    PCollection<ReadableFile> datastreamFiles =
        input.apply("Read Datastream Files", new DataStreamFileIO());
    PCollection<FailsafeElement<String, String>> datastreamJsonStrings =
        expandDataStreamJsonStrings(datastreamFiles);

    return datastreamJsonStrings;
  }

  public PCollection<FailsafeElement<String, String>> expandDataStreamJsonStrings(
      PCollection<ReadableFile> datastreamFiles) {
    PCollection<FailsafeElement<String, String>> datastreamRecords;

    FailsafeElementCoder coder =
        FailsafeElementCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of());

      SerializableFunction<GenericRecord, FailsafeElement<String, String>> parseFn =
          FormatDatastreamRecordToJson.create()
              .withStreamName(this.streamName)
              .withRenameColumnValues(this.renameColumns)
              .withHashRowId(this.hashRowId)
              .withLowercaseSourceColumns(this.lowercaseSourceColumns);
      datastreamRecords =
          datastreamFiles
              .apply("ReshuffleFiles", Reshuffle.<ReadableFile>viaRandomKey())
              .apply(
                  "ParseAvroRows",
                  ParDo.of(
                      new ReadFileRangesFn<FailsafeElement<String, String>>(
                          new CreateParseSourceFn(parseFn, coder),
                          new ReadFileRangesFn.ReadFileRangesFnExceptionHandler())))
              .setCoder(coder);
    return datastreamRecords.apply("Reshuffle", Reshuffle.viaRandomKey());
  }

  private static class CreateParseSourceFn
      implements SerializableFunction<String, FileBasedSource<FailsafeElement<String, String>>> {
    private final SerializableFunction<GenericRecord, FailsafeElement<String, String>> parseFn;
    private final Coder<FailsafeElement<String, String>> coder;

    CreateParseSourceFn(
        SerializableFunction<GenericRecord, FailsafeElement<String, String>> parseFn,
        Coder<FailsafeElement<String, String>> coder) {
      this.parseFn = parseFn;
      this.coder = coder;
    }

    @Override
    public FileBasedSource<FailsafeElement<String, String>> apply(String input) {
      return AvroSource.from(input).withParseFn(parseFn, coder);
    }
  }

  class DataStreamFileIO extends PTransform<PBegin, PCollection<ReadableFile>> {

    @Override
    public PCollection<ReadableFile> expand(PBegin input) {
      PCollection<ReadableFile> datastreamFiles;
      datastreamFiles = expandBatchReadPipeline(input);
      return datastreamFiles;
    }

    public PCollection<ReadableFile> expandBatchReadPipeline(PBegin input) {
      return input
          .apply("MatchFiles", FileIO.match().filepattern(inputFilePattern + "**")) //wildcard to match everything inside the folder
          .apply("ReadFiles", FileIO.readMatches());
    }
  }
}
