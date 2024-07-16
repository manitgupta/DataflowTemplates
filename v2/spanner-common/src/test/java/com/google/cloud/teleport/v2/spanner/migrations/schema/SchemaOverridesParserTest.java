package com.google.cloud.teleport.v2.spanner.migrations.schema;

import static junit.framework.TestCase.assertEquals;

import java.util.HashMap;
import java.util.Map;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.Test;

public class SchemaOverridesParserTest {

  SchemaOverridesParser schemaOverridesParser;

  @Test
  public void testGetTableOverride() {
    Map<String, String> userOptionsOverrides = new HashMap<>();
    userOptionsOverrides.put("tableOverrides", "[{Singers, Vocalists}, {Albums, Records},{Hello, World}]");
    schemaOverridesParser = new SchemaOverridesParser(userOptionsOverrides);
    String sourceTableName = "Singers";
    String result = schemaOverridesParser.getTableOverrideOrDefault(sourceTableName);
    assertEquals(3, schemaOverridesParser.tableNameOverrides.keySet().size());
    assertEquals("Vocalists", result);
  }

  @Test
  public void testGetColumnOverride() {
    Map<String, String> userOptionsOverrides = new HashMap<>();
    userOptionsOverrides.put("columnOverrides", "[{Singers.SingerName, Vocalists.TalentName}, {Albums.AlbumName, Records.RecordName}]");
    schemaOverridesParser = new SchemaOverridesParser(userOptionsOverrides);
    String sourceTableName = "Singers";
    String sourceColumnName = "SingerName";
    Pair<String, String> result = schemaOverridesParser.getColumnOverrideOrDefault(sourceTableName, sourceColumnName);
    assertEquals(2, schemaOverridesParser.columnNameOverrides.keySet().size());
    assertEquals("Vocalists", result.getLeft());
    assertEquals("TalentName", result.getRight());
  }

  @Test
  public void testGetTableAndColumnOverride() {
    Map<String, String> userOptionsOverrides = new HashMap<>();
    userOptionsOverrides.put("tableOverrides", "[{Singers, Vocalists}, {Albums, Records},{Hello, World}]");
    userOptionsOverrides.put("columnOverrides", "[{Singers.SingerName, Vocalists.TalentName}, {Albums.AlbumName, Records.RecordName}]");
    schemaOverridesParser = new SchemaOverridesParser(userOptionsOverrides);
    String sourceTableName = "Singers";
    String sourceColumnName = "SingerName";
    String tableResult = schemaOverridesParser.getTableOverrideOrDefault(sourceTableName);
    Pair<String, String> columnResult = schemaOverridesParser.getColumnOverrideOrDefault(sourceTableName, sourceColumnName);
    assertEquals(3, schemaOverridesParser.tableNameOverrides.keySet().size());
    assertEquals("Vocalists", tableResult);
    assertEquals(2, schemaOverridesParser.columnNameOverrides.keySet().size());
    assertEquals("Vocalists", columnResult.getLeft());
    assertEquals("TalentName", columnResult.getRight());
  }

  @Test
  public void testGetDefaultTableOverrides() {
    Map<String, String> userOptionsOverrides = new HashMap<>();
    userOptionsOverrides.put("tableOverrides", "[{Singers, Vocalists}, {Albums, Records}]");
    schemaOverridesParser = new SchemaOverridesParser(userOptionsOverrides);
    String sourceTableName = "Labels";
    String result = schemaOverridesParser.getTableOverrideOrDefault(sourceTableName);
    assertEquals(sourceTableName, result);
  }

  @Test
  public void testGetDefaultColumnOverrides() {
    Map<String, String> userOptionsOverrides = new HashMap<>();
    userOptionsOverrides.put("columnOverrides", "[{Singers.SingerName, Vocalists.TalentName}, {Albums.AlbumName, Records.RecordName}]");
    schemaOverridesParser = new SchemaOverridesParser(userOptionsOverrides);
    String sourceTableName = "Labels";
    String sourceColumnName = "Owners";
    Pair<String, String> result = schemaOverridesParser.getColumnOverrideOrDefault(sourceTableName, sourceColumnName);
    assertEquals(2, schemaOverridesParser.columnNameOverrides.keySet().size());
    assertEquals("Labels", result.getLeft());
    assertEquals("Owners", result.getRight());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testMalformedGetTableOverrides() {
    Map<String, String> userOptionsOverrides = new HashMap<>();
    userOptionsOverrides.put("tableOverrides", "[{Singers}}, {Albums, Records}]");
    schemaOverridesParser = new SchemaOverridesParser(userOptionsOverrides);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testMalformedGetColumnOverrides() {
    Map<String, String> userOptionsOverrides = new HashMap<>();
    userOptionsOverrides.put("columnOverrides", "[{Singers, Vocalists}, {Albums.AlbumName, Records.RecordName}]");
    schemaOverridesParser = new SchemaOverridesParser(userOptionsOverrides);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testConsistentTableAndColumnOverrides() {
    Map<String, String> userOptionsOverrides = new HashMap<>();
    userOptionsOverrides.put("tableOverrides", "[{Singers, Vocalists}, {Albums, Records},{Hello, World}]");
    userOptionsOverrides.put("columnOverrides", "[{Singers.SingerName, Pianists.TalentName}, {Albums.AlbumName, Records.RecordName}]");
    schemaOverridesParser = new SchemaOverridesParser(userOptionsOverrides);
  }
}