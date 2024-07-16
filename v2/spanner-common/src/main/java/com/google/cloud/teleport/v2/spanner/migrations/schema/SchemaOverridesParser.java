package com.google.cloud.teleport.v2.spanner.migrations.schema;

import com.google.common.annotations.VisibleForTesting;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

/**
 * Encodes the user specified schema overrides for retrieval during source to spanner mapping.
 */
public class SchemaOverridesParser implements Serializable {

  @VisibleForTesting
  final Map<String, String> tableNameOverrides;
  @VisibleForTesting
  final Map<String, Pair<String, String>> columnNameOverrides;

  public SchemaOverridesParser(Map<String, String> userOptionOverrides) {
    tableNameOverrides = new HashMap<>();
    columnNameOverrides = new HashMap<>();
    if (userOptionOverrides.containsKey("tableOverrides")) {
      parseTableMapping(userOptionOverrides.get("tableOverrides"));
    }
    if (userOptionOverrides.containsKey("columnOverrides")) {
      parseColumnMapping(userOptionOverrides.get("columnOverrides"));
    }
  }

  /**
   * Gets the spanner table name given the source table name
   *
   * @param sourceTableName The source table name
   * @return The overridden spanner table name
   */
  public String getTableOverrideOrDefault(String sourceTableName) {
    return tableNameOverrides.getOrDefault(sourceTableName, sourceTableName);
  }

  /**
   * Gets the spanner column name given the source table name
   *
   * @param sourceTableName the source table name for which column name is overridden
   * @param sourceColumnName the source column name being overridden
   * @return A pair of spannerTableName and spannerColumnName
   */
  public Pair<String, String> getColumnOverrideOrDefault(String sourceTableName,
      String sourceColumnName) {
    return columnNameOverrides.getOrDefault(
        String.format("%s.%s", sourceTableName, sourceColumnName),
        new ImmutablePair<>(sourceTableName, sourceColumnName));
  }

  private void parseTableMapping(String tableInput) {
    String pairsString = tableInput.substring(1, tableInput.length() - 1);
    int openBraceCount = 0;
    int startIndex = 0;
    for (int i = 0; i < pairsString.length(); i++) {
      char c = pairsString.charAt(i);
      if (c == '{') {
        openBraceCount++;
        if (openBraceCount == 1) {
          startIndex = i;
        }
      } else if (c == '}') {
        openBraceCount--;
        if (openBraceCount == 0) {
          String pair = pairsString.substring(startIndex + 1, i);
          String[] tables = pair.split(",");
          if (tables.length == 2) {
            String sourceTable = tables[0].trim();
            String destinationTable = tables[1].trim();
            tableNameOverrides.put(sourceTable, destinationTable);
          } else {
            throw new IllegalArgumentException(String.format(
                "Malformed pair encountered: %s, please fix the tableOverrides and rerun the job.",
                pair));
          }
        }
      }
    }
  }

  private void parseColumnMapping(String columnInput) {
    String pairsString = columnInput.substring(1, columnInput.length() - 1);
    int openBraceCount = 0;
    int startIndex = 0;
    for (int i = 0; i < pairsString.length(); i++) {
      char c = pairsString.charAt(i);
      if (c == '{') {
        openBraceCount++;
        if (openBraceCount == 1) {
          startIndex = i;
        }
      } else if (c == '}') {
        openBraceCount--;
        if (openBraceCount == 0) {
          String pair = pairsString.substring(startIndex + 1, i);
          String[] tableColumnPairs = pair.split(",");
          if (tableColumnPairs.length == 2) {
            String[] sourceTableColumn = tableColumnPairs[0].trim().split("\\.");
            String[] destTableColumn = tableColumnPairs[1].trim().split("\\.");
            if (sourceTableColumn.length == 2 && destTableColumn.length == 2) {
              columnNameOverrides.put(
                  String.format("%s.%s", sourceTableColumn[0], sourceTableColumn[1]),
                  new ImmutablePair<>(destTableColumn[0], destTableColumn[1]));
            } else {
              throw new IllegalArgumentException(String.format(
                  "Malformed pair encountered: %s, please fix the columnOverrides and rerun the job.",
                  pair));
            }
          } else {
            throw new IllegalArgumentException(String.format(
                "Malformed pair encountered: %s, please fix the columnOverrides and rerun the job.",
                pair));
          }
        }
      }
    }
  }
}
