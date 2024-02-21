package com.google.cloud.teleport.v2.templates;

import static com.google.cloud.teleport.v2.templates.datastream.DatastreamConstants.EVENT_SCHEMA_KEY;
import static com.google.cloud.teleport.v2.templates.datastream.DatastreamConstants.EVENT_TABLE_NAME_KEY;
import static com.google.cloud.teleport.v2.templates.datastream.DatastreamConstants.EVENT_UUID_KEY;
import static com.google.cloud.teleport.v2.templates.datastream.DatastreamConstants.MYSQL_SOURCE_TYPE;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.teleport.v2.spanner.ddl.Ddl;
import com.google.cloud.teleport.v2.spanner.migrations.schema.NameAndCols;
import com.google.cloud.teleport.v2.spanner.migrations.schema.Schema;
import com.google.cloud.teleport.v2.spanner.migrations.schema.SourceColumnDefinition;
import com.google.cloud.teleport.v2.spanner.migrations.schema.SourceTable;
import com.google.cloud.teleport.v2.spanner.migrations.schema.SpannerColumnDefinition;
import com.google.cloud.teleport.v2.spanner.migrations.schema.SpannerTable;
import com.google.cloud.teleport.v2.spanner.migrations.schema.SyntheticPKey;
import com.google.cloud.teleport.v2.spanner.migrations.transformation.TransformationContext;
import com.google.cloud.teleport.v2.templates.datastream.ChangeEventContext;
import com.google.cloud.teleport.v2.templates.datastream.ChangeEventContextFactory;
import com.google.cloud.teleport.v2.values.FailsafeElement;
import java.io.Serializable;
import java.util.Map;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class DataStreamRecordsToSpannerMutationsDoFn extends DoFn<FailsafeElement<String, String>, Mutation>
    implements Serializable {

  private static final Logger LOG = LoggerFactory.getLogger(DataStreamRecordsToSpannerMutationsDoFn.class);
  private final TransformationContext transformationContext;
  private final PCollectionView<Ddl> ddlView;

  private final Schema schema;
  private ObjectMapper mapper;

  private final String sourceType;

  public DataStreamRecordsToSpannerMutationsDoFn(TransformationContext transformationContext, PCollectionView<Ddl> ddlView, Schema schema, String sourceType) {
    this.transformationContext = transformationContext;
    this.ddlView = ddlView;
    this.schema = schema;
    this.sourceType = sourceType;
  }

  @Setup
  public void setup() {
    mapper = new ObjectMapper();
    mapper.enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS);
  }

  @ProcessElement
  public void processElement(ProcessContext c) {
    FailsafeElement<String, String> msg = c.element();
    Ddl ddl = c.sideInput(ddlView);
    boolean isRetryRecord = false;
    /*
     * Try Catch block to capture any exceptions that might occur while processing
     * DataStream events while writing to Cloud Spanner. All Exceptions that are caught
     * can be retried based on the exception type.
     */
    try {

      JsonNode changeEvent = mapper.readTree(msg.getPayload());
      if (!schema.isEmpty()) {
        verifyTableInSession(changeEvent.get(EVENT_TABLE_NAME_KEY).asText());
        changeEvent = transformChangeEventViaSessionFile(changeEvent);
        // Sequence information for the current change event.

        ChangeEventContext changeEventContext =
            ChangeEventContextFactory.createChangeEventContext(
                changeEvent, ddl, "", sourceType);
        c.output(changeEventContext.getDataMutation());
      }
    } catch (IllegalStateException e) {
      LOG.warn(e.getMessage());
      outputWithErrorTag(c, msg, e, SpannerTransactionWriter.PERMANENT_ERROR_TAG);
    } catch (Exception e) {
      LOG.warn(e.getMessage());
      outputWithErrorTag(c, msg, e, SpannerTransactionWriter.RETRYABLE_ERROR_TAG);
    }
  }

  void outputWithErrorTag(
      ProcessContext c,
      FailsafeElement<String, String> changeEvent,
      Exception e,
      TupleTag<FailsafeElement<String, String>> errorTag) {
    // Making a copy, as the input must not be mutated.
    FailsafeElement<String, String> output = FailsafeElement.of(changeEvent);
    output.setErrorMessage(e.getMessage());
    c.output(errorTag, output);
  }
  void verifyTableInSession(String tableName)
      throws IllegalArgumentException, DroppedTableException {
    if (!schema.getSrcToID().containsKey(tableName)) {
      throw new IllegalArgumentException(
          "Missing entry for " + tableName + " in srcToId map, provide a valid session file.");
    }
    if (!schema.getToSpanner().containsKey(tableName)) {
      throw new DroppedTableException(
          "Cannot find entry for "
              + tableName
              + " in toSpanner map, it is likely this table was dropped");
    }
    String tableId = schema.getSrcToID().get(tableName).getName();
    if (!schema.getSpSchema().containsKey(tableId)) {
      throw new IllegalArgumentException(
          "Missing entry for " + tableId + " in spSchema, provide a valid session file.");
    }
  }

  JsonNode transformChangeEventViaSessionFile(JsonNode changeEvent) {
    String tableName = changeEvent.get(EVENT_TABLE_NAME_KEY).asText();
    String tableId = schema.getSrcToID().get(tableName).getName();

    // Convert table and column names in change event.
    changeEvent = convertTableAndColumnNames(changeEvent, tableName);

    // Add synthetic PK to change event.
    changeEvent = addSyntheticPKs(changeEvent, tableId);

    // Remove columns present in change event that were dropped in Spanner.
    changeEvent = removeDroppedColumns(changeEvent, tableId);

    // Add shard id to change event.
    changeEvent = populateShardId(changeEvent, tableId);

    return changeEvent;
  }

  JsonNode convertTableAndColumnNames(JsonNode changeEvent, String tableName) {
    NameAndCols nameAndCols = schema.getToSpanner().get(tableName);
    String spTableName = nameAndCols.getName();
    Map<String, String> cols = nameAndCols.getCols();

    // Convert the table name to corresponding Spanner table name.
    ((ObjectNode) changeEvent).put(EVENT_TABLE_NAME_KEY, spTableName);
    // Convert the column names to corresponding Spanner column names.
    for (Map.Entry<String, String> col : cols.entrySet()) {
      String srcCol = col.getKey(), spCol = col.getValue();
      if (!srcCol.equals(spCol)) {
        ((ObjectNode) changeEvent).set(spCol, changeEvent.get(srcCol));
        ((ObjectNode) changeEvent).remove(srcCol);
      }
    }
    return changeEvent;
  }

  JsonNode addSyntheticPKs(JsonNode changeEvent, String tableId) {
    Map<String, SpannerColumnDefinition> spCols = schema.getSpSchema().get(tableId).getColDefs();
    Map<String, SyntheticPKey> synthPks = schema.getSyntheticPks();
    if (synthPks.containsKey(tableId)) {
      String colID = synthPks.get(tableId).getColId();
      if (!spCols.containsKey(colID)) {
        throw new IllegalArgumentException(
            "Missing entry for "
                + colID
                + " in colDefs for tableId: "
                + tableId
                + ", provide a valid session file.");
      }
      ((ObjectNode) changeEvent)
          .put(spCols.get(colID).getName(), changeEvent.get(EVENT_UUID_KEY).asText());
    }
    return changeEvent;
  }

  JsonNode removeDroppedColumns(JsonNode changeEvent, String tableId) {
    Map<String, SpannerColumnDefinition> spCols = schema.getSpSchema().get(tableId).getColDefs();
    SourceTable srcTable = schema.getSrcSchema().get(tableId);
    Map<String, SourceColumnDefinition> srcCols = srcTable.getColDefs();
    for (String colId : srcTable.getColIds()) {
      // If spanner columns do not contain this column Id, drop from change event.
      if (!spCols.containsKey(colId)) {
        ((ObjectNode) changeEvent).remove(srcCols.get(colId).getName());
      }
    }
    return changeEvent;
  }

  JsonNode populateShardId(JsonNode changeEvent, String tableId) {
    if (!MYSQL_SOURCE_TYPE.equals(this.sourceType)
        || transformationContext.getSchemaToShardId() == null
        || transformationContext.getSchemaToShardId().isEmpty()) {
      return changeEvent; // Nothing to do
    }

    SpannerTable table = schema.getSpSchema().get(tableId);
    String shardIdColumn = table.getShardIdColumn();
    if (shardIdColumn == null) {
      return changeEvent;
    }
    SpannerColumnDefinition shardIdColDef = table.getColDefs().get(table.getShardIdColumn());
    if (shardIdColDef == null) {
      return changeEvent;
    }
    Map<String, String> schemaToShardId = transformationContext.getSchemaToShardId();
    String schemaName = changeEvent.get(EVENT_SCHEMA_KEY).asText();
    String shardId = schemaToShardId.get(schemaName);
    ((ObjectNode) changeEvent).put(shardIdColDef.getName(), shardId);
    return changeEvent;
  }

}
