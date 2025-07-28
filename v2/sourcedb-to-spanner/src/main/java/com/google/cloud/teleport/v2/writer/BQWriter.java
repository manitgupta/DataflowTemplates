package com.google.cloud.teleport.v2.writer;

import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.teleport.v2.spanner.migrations.schema.ISchemaMapper;
import com.google.cloud.teleport.v2.templates.RowContext;
import java.io.Serializable;
import java.util.Map;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.TableDestination;
import org.apache.beam.sdk.io.gcp.bigquery.WriteResult;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.ValueInSingleWindow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BQWriter implements Serializable {

  private static final Logger LOG = LoggerFactory.getLogger(BQWriter.class);

  private final String projectId;

  private final String datasetName;

  private final PCollectionView<Map<String, String>> bqSchemaView;

  private final ISchemaMapper schemaMapper;

  public BQWriter(String projectId, String datasetName, PCollectionView<Map<String, String>> bqSchemaView, ISchemaMapper schemaMapper) {
    this.projectId = projectId;
    this.datasetName = datasetName;
    this.bqSchemaView = bqSchemaView;
    this.schemaMapper = schemaMapper;
  }

  public WriteResult writeToBQ(PCollection<RowContext> rows) {
    LOG.info("initiating write to BQ");
    return rows.apply("WriteToBQ", BigQueryIO.<RowContext>write()
        .to(new SerializableFunction<ValueInSingleWindow<RowContext>, TableDestination>() {
          @Override
          public TableDestination apply(ValueInSingleWindow<RowContext> input) {
            String tableSpec = String.format("%s:%s.%s", projectId, datasetName, schemaMapper.getSpannerTableName("", input.getValue().row().tableName()));
            return new TableDestination(tableSpec, String.format("Destination for %s", tableSpec));
          }
        })
        .withFormatFunction((RowContext rowContext) -> {
          TableRow tableRow = new TableRow();
          for (String colName: rowContext.tableRow().getInternalMap().keySet()) {
            tableRow.set(colName, rowContext.tableRow().getInternalMap().get(colName));
          }
          return tableRow;
        })
        .withSchemaFromView(bqSchemaView)
        .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
        .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND));
  }
}
