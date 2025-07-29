package com.google.cloud.teleport.v2.transformer;

import com.google.auto.value.AutoValue;
import com.google.cloud.teleport.v2.constants.MetricCounters;
import com.google.cloud.teleport.v2.constants.SourceDbToSpannerConstants;
import com.google.cloud.teleport.v2.source.reader.io.row.SourceRow;
import com.google.cloud.teleport.v2.spanner.migrations.avro.GenericRecordTypeConvertor;
import com.google.cloud.teleport.v2.spanner.migrations.schema.ISchemaMapper;
import com.google.cloud.teleport.v2.spanner.migrations.transformation.CustomTransformation;
import com.google.cloud.teleport.v2.spanner.migrations.utils.CustomTransformationImplFetcher;
import com.google.cloud.teleport.v2.spanner.utils.ISpannerMigrationTransformer;
import com.google.cloud.teleport.v2.templates.RowContext;
import java.io.Serializable;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@AutoValue
public abstract class SourceRowToTableRowDoFn extends DoFn<SourceRow, RowContext> implements
    Serializable {

  private static final Logger LOG = LoggerFactory.getLogger(SourceRowToTableRowDoFn.class);

  private transient ISpannerMigrationTransformer sourceDbToSpannerTransformer;

  public void setSourceDbToSpannerTransformer(
      ISpannerMigrationTransformer sourceDbToSpannerTransformer) {
    this.sourceDbToSpannerTransformer = sourceDbToSpannerTransformer;
  }

  private final Counter transformerErrors =
      Metrics.counter(SourceRowToMutationDoFn.class, MetricCounters.TRANSFORMER_ERRORS);

  private final Counter filteredEvents =
      Metrics.counter(SourceRowToMutationDoFn.class, MetricCounters.FILTERED_EVENTS);

  public abstract ISchemaMapper iSchemaMapper();

  @Nullable
  public abstract CustomTransformation customTransformation();

  public static SourceRowToTableRowDoFn create(
      ISchemaMapper iSchemaMapper, CustomTransformation customTransformation) {
    return new AutoValue_SourceRowToTableRowDoFn(iSchemaMapper, customTransformation);
  }

  /** Setup function to load custom transformation jars. */
  @Setup
  public void setup() {
    sourceDbToSpannerTransformer =
        CustomTransformationImplFetcher.getCustomTransformationLogicImpl(customTransformation());
  }

  @ProcessElement
  public void processElement(ProcessContext c, MultiOutputReceiver output) {
    SourceRow sourceRow = c.element();
    LOG.debug("Starting transformation for Source Row {}", sourceRow);

    try {
      // TODO: update namespace in constructor when Spanner namespace support is added.
      GenericRecord record = sourceRow.getPayload();
      String srcTableName = sourceRow.tableName();
      GenericRecordTypeConvertor genericRecordTypeConvertor =
          new GenericRecordTypeConvertor(
              iSchemaMapper(), "", sourceRow.shardId(), sourceDbToSpannerTransformer);
      Map<String, Object> values =
          genericRecordTypeConvertor.transformEventForBQ(record, srcTableName);
      if (values == null) {
        filteredEvents.inc();
        output
            .get(SourceDbToSpannerConstants.FILTERED_EVENT_TAG)
            .output(RowContext.builder().setRow(sourceRow).build());
        return;
      }
      TempTableRow tableRow = tableRowFromMap(values);
      output.get(SourceDbToSpannerConstants.ROW_TRANSFORMATION_SUCCESS)
          .output(RowContext.builder().setTableRow(tableRow).build());
    } catch (Exception e) {
      LOG.error("Error while processing element", e);
      transformerErrors.inc();
      output
          .get(SourceDbToSpannerConstants.ROW_TRANSFORMATION_ERROR)
          .output(RowContext.builder().setRow(sourceRow).setErr(e).build());
    }
  }

  private TempTableRow tableRowFromMap(Map<String, Object> values) {
    TempTableRow row = new TempTableRow();
    for (String spannerColName : values.keySet()) {
      Object value = values.get(spannerColName);
      row.set(spannerColName, value);
    }
    return row;
  }
}
