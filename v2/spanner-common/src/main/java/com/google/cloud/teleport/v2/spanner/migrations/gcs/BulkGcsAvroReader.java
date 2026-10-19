/*
 * Copyright (C) 2026 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.google.cloud.teleport.v2.spanner.migrations.gcs;

import com.google.cloud.teleport.v2.coders.GenericRecordCoder;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import java.util.Collection;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.extensions.avro.io.AvroIO;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.fs.EmptyMatchTreatment;
import org.apache.beam.sdk.io.fs.MatchResult;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.Wait;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;

/**
 * Reads the records of a bulk migration Avro export (see {@link BulkGcsFormat}) as {@link
 * GenericRecord}s, each with the schema of the file it was read from.
 *
 * <p>Pipeline: file patterns ({@link BulkGcsFormat#filePatterns}) → {@code FileIO.matchAll} (empty
 * matches allowed) → optional {@link BulkGcsFormat#isConfiguredFile} filter → {@code
 * FileIO.readMatches} (directories prohibited) → {@code AvroIO.parseFilesGenericRecords} with
 * {@link GenericRecordCoder}.
 */
public final class BulkGcsAvroReader extends PTransform<PBegin, PCollection<GenericRecord>> {

  private final String root;
  private final ImmutableSet<String> tables;
  private final ImmutableSet<String> shardIds;
  @Nullable private final Wait.OnSignal<?> waitOnSignal;

  private BulkGcsAvroReader(
      String root,
      ImmutableSet<String> tables,
      ImmutableSet<String> shardIds,
      @Nullable Wait.OnSignal<?> waitOnSignal) {
    this.root = BulkGcsFormat.stripTrailingSlash(root);
    this.tables = tables;
    this.shardIds = shardIds;
    this.waitOnSignal = waitOnSignal;
  }

  /**
   * Creates a reader.
   *
   * @param root root directory of the export; a trailing {@code /} is ignored.
   * @param tables tables to read; empty or null means all tables.
   * @param shardIds shards to read; empty or null means all shards.
   */
  public static BulkGcsAvroReader of(
      String root, @Nullable Collection<String> tables, @Nullable Collection<String> shardIds) {
    if (root == null || root.trim().isEmpty()) {
      throw new IllegalArgumentException("Root directory of the Avro export must not be blank.");
    }
    return new BulkGcsAvroReader(
        root,
        tables == null ? ImmutableSet.of() : ImmutableSet.copyOf(tables),
        shardIds == null ? ImmutableSet.of() : ImmutableSet.copyOf(shardIds),
        null);
  }

  /** Returns a copy of this reader that lists files only after {@code signal} has fired. */
  public BulkGcsAvroReader withWaitOnSignal(@Nullable Wait.OnSignal<?> signal) {
    return new BulkGcsAvroReader(root, tables, shardIds, signal);
  }

  /** Returns the file patterns listed by this reader. */
  public List<String> getFilePatterns() {
    return ImmutableList.copyOf(BulkGcsFormat.filePatterns(root, tables, shardIds));
  }

  @Override
  public PCollection<GenericRecord> expand(PBegin input) {
    PCollection<String> patterns =
        input.apply(
            "CreateFilePatterns", Create.of(getFilePatterns()).withCoder(StringUtf8Coder.of()));
    if (waitOnSignal != null) {
      patterns = waitOn(patterns, waitOnSignal);
    }
    PCollection<MatchResult.Metadata> files =
        patterns.apply(
            "MatchFilePatterns",
            FileIO.matchAll().withEmptyMatchTreatment(EmptyMatchTreatment.ALLOW));
    if (BulkGcsFormat.needsFileFilter(tables, shardIds)) {
      String filterRoot = root;
      ImmutableSet<String> filterTables = tables;
      ImmutableSet<String> filterShards = shardIds;
      files =
          files.apply(
              "KeepConfiguredFiles",
              Filter.by(
                  file ->
                      BulkGcsFormat.isConfiguredFile(
                          filterRoot, filterTables, filterShards, file.resourceId().toString())));
    }
    return files
        .apply(
            "ReadMatchedFiles",
            FileIO.readMatches()
                .withDirectoryTreatment(FileIO.ReadMatches.DirectoryTreatment.PROHIBIT))
        .apply(
            "ReadSourceAvroRecords",
            AvroIO.parseFilesGenericRecords(new IdentityGenericRecordFn())
                .withCoder(GenericRecordCoder.of()));
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  private static PCollection<String> waitOn(PCollection<String> seed, Wait.OnSignal<?> signal) {
    // Wait.OnSignal is typed on the main input's element type but passes any element through.
    return (PCollection<String>) ((PCollection) seed).apply("WaitOnSignal", (PTransform) signal);
  }
}
