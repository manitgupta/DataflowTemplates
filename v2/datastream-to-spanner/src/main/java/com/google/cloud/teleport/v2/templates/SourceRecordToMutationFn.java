package com.google.cloud.teleport.v2.templates;

import com.google.cloud.ByteArray;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.Value;
import java.math.BigDecimal;
import java.util.Objects;
import org.apache.beam.sdk.transforms.DoFn;

/**
 * A DoFn to convert a generic SourceRecord into a Spanner Mutation.
 * This assumes an "insert-or-update" operation for idempotency.
 */
public class SourceRecordToMutationFn extends DoFn<SourceRecord, Mutation> {

  @ProcessElement
  public void processElement(ProcessContext c) {
    SourceRecord record = c.element();
    if (record == null || record.length() == 0) {
      return;
    }

    // Assumption: The target table name is stored as a field in the SourceRecord.
    SourceField tableField = record.getField("source_table");
    Objects.requireNonNull(tableField, "SourceRecord must contain a 'source_table' field.");
    String tableName = (String) tableField.getFieldValue();

    // Use newInsertOrUpdateBuilder for idempotent writes, which is safer for retries.
    Mutation.WriteBuilder builder = Mutation.newInsertOrUpdateBuilder(tableName);

    // Iterate through fields and build the mutation
    for (int i = 0; i < record.length(); i++) {
      SourceField field = record.getField(i);
      String fieldName = field.getFieldName();
      Object fieldValue = field.getFieldValue();
      String fieldType = field.getFieldDataType().toUpperCase();

      // Skip the source_table field itself as it's not a column in the target table.
      if (fieldName.equals("source_table")) {
        continue;
      }

      // This switch handles mapping from JDBC types to Spanner types.
      // It needs to be as comprehensive as your data requires.
      switch (fieldType) {
        case "STRING":
        case "VARCHAR":
        case "TEXT":
          builder.set(fieldName).to((String) fieldValue);
          break;
        case "INT":
        case "INTEGER":
          builder.set(fieldName).to((Integer) fieldValue);
          break;
        case "BIGINT":
          builder.set(fieldName).to((Long) fieldValue);
          break;
        case "DOUBLE":
        case "FLOAT":
          builder.set(fieldName).to((Double) fieldValue);
          break;
        case "NUMERIC":
        case "DECIMAL":
          builder.set(fieldName).to((BigDecimal) fieldValue);
          break;
        case "BOOL":
        case "BOOLEAN":
          builder.set(fieldName).to((Boolean) fieldValue);
          break;
        case "DATE":
          builder.set(fieldName).to(Value.date((com.google.cloud.Date) fieldValue));
          break;
        case "TIMESTAMP":
          builder.set(fieldName).to(Value.timestamp((com.google.cloud.Timestamp) fieldValue));
          break;
        case "BYTES":
          builder.set(fieldName).to(Value.bytes(ByteArray.copyFrom((byte[]) fieldValue)));
          break;
        default:
          // It's good practice to handle unknown types
          throw new IllegalArgumentException("Unsupported field data type: " + fieldType);
      }
    }
    c.output(builder.build());
  }
}
