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
package com.google.cloud.teleport.v2.source.gcs;

import static com.google.common.truth.Truth.assertThat;

import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.Value;
import com.google.cloud.teleport.v2.constants.SourceDbToSpannerConstants;
import com.google.cloud.teleport.v2.reader.ReaderImpl;
import com.google.cloud.teleport.v2.reader.io.row.SourceRow;
import com.google.cloud.teleport.v2.reader.io.transform.ReaderTransform;
import com.google.cloud.teleport.v2.source.gcs.iowrapper.GcsIoWrapper;
import com.google.cloud.teleport.v2.spanner.ddl.Ddl;
import com.google.cloud.teleport.v2.spanner.migrations.schema.IdentityMapper;
import com.google.cloud.teleport.v2.templates.RowContext;
import com.google.cloud.teleport.v2.transformer.SourceRowToMutationDoFn;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTagList;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * End-to-end test of the GCS source up to Spanner mutations: bulk-format Avro export -> {@link
 * GcsIoWrapper} -> {@link SourceRowToMutationDoFn} with an {@link IdentityMapper}.
 */
@RunWith(JUnit4.class)
public class GcsSourceToMutationTest {

  @Rule public final transient TestPipeline pipeline = TestPipeline.create();
  @Rule public TemporaryFolder tmp = new TemporaryFolder();

  private static Ddl ddl() {
    return Ddl.builder()
        .createTable("user_profiles")
        .column("key")
        .string()
        .size(254)
        .notNull()
        .endColumn()
        .column("id")
        .string()
        .max()
        .endColumn()
        .column("rev")
        .string()
        .size(64)
        .endColumn()
        .column("doc")
        .json()
        .notNull()
        .endColumn()
        .column("name")
        .string()
        .max()
        .generatedAs("JSON_VALUE(doc, '$.name')")
        .stored()
        .endColumn()
        .primaryKey()
        .asc("key")
        .end()
        .endTable()
        .createTable("knows")
        .column("key")
        .string()
        .size(254)
        .notNull()
        .endColumn()
        .column("id")
        .string()
        .max()
        .endColumn()
        .column("rev")
        .string()
        .size(64)
        .endColumn()
        .column("from_id")
        .string()
        .max()
        .notNull()
        .endColumn()
        .column("to_id")
        .string()
        .max()
        .notNull()
        .endColumn()
        .column("doc")
        .json()
        .notNull()
        .endColumn()
        .column("from_key")
        .string()
        .size(254)
        .generatedAs("REGEXP_EXTRACT(from_id, r'^[^/]+/(.*)$')")
        .stored()
        .endColumn()
        .primaryKey()
        .asc("key")
        .end()
        .endTable()
        .build();
  }

  /** Mutation fields as a stable, comparable string. */
  private static String describe(Mutation m) {
    List<String> parts = new ArrayList<>();
    for (Map.Entry<String, Value> e : m.asMap().entrySet()) {
      Value v = e.getValue();
      parts.add(
          e.getKey() + ":" + v.getType().getCode() + "=" + (v.isNull() ? "NULL" : v.toString()));
    }
    parts.sort(String::compareTo);
    return m.getOperation() + " " + m.getTable() + " " + String.join(",", parts);
  }

  @Test
  public void testExportToMutations() throws Exception {
    String root = GcsTestFixtures.writeExport(tmp.getRoot());
    ReaderImpl reader = ReaderImpl.of(new GcsIoWrapper(root, List.of(), null));
    ReaderTransform readerTransform = reader.getReaderTransform();

    PCollectionTuple read = pipeline.apply("Read", readerTransform.readTransform());
    PCollection<SourceRow> rows = read.get(readerTransform.sourceRowTag());

    PCollectionTuple transformed =
        rows.apply(
            "Transform",
            ParDo.of(SourceRowToMutationDoFn.create(new IdentityMapper(ddl()), null, false))
                .withOutputTags(
                    SourceDbToSpannerConstants.ROW_TRANSFORMATION_SUCCESS,
                    TupleTagList.of(SourceDbToSpannerConstants.ROW_TRANSFORMATION_ERROR)
                        .and(SourceDbToSpannerConstants.FILTERED_EVENT_TAG)));
    transformed
        .get(SourceDbToSpannerConstants.ROW_TRANSFORMATION_ERROR)
        .setCoder(SerializableCoder.of(RowContext.class));
    transformed
        .get(SourceDbToSpannerConstants.FILTERED_EVENT_TAG)
        .setCoder(SerializableCoder.of(RowContext.class));
    PCollection<String> mutations =
        transformed
            .get(SourceDbToSpannerConstants.ROW_TRANSFORMATION_SUCCESS)
            .setCoder(SerializableCoder.of(RowContext.class))
            .apply(
                "Describe",
                MapElements.into(TypeDescriptor.of(String.class))
                    .via((RowContext r) -> describe(r.mutation())));
    PAssert.that(transformed.get(SourceDbToSpannerConstants.ROW_TRANSFORMATION_ERROR)).empty();

    PAssert.that(mutations)
        .containsInAnyOrder(
            describe(
                Mutation.newInsertOrUpdateBuilder("user_profiles")
                    .set("key")
                    .to("u1")
                    .set("id")
                    .to("user_profiles/u1")
                    .set("rev")
                    .to("_revu1")
                    .set("doc")
                    .to(Value.json("{\"_key\":\"u1\",\"name\":\"Ann\",\"tags\":[1,\"x\",null]}"))
                    .build()),
            describe(
                Mutation.newInsertOrUpdateBuilder("user_profiles")
                    .set("key")
                    .to("u2")
                    .set("id")
                    .to("user_profiles/u2")
                    .set("rev")
                    .to("_revu2")
                    .set("doc")
                    .to(Value.json("{\"_key\":\"u2\",\"nested\":{\"a\":{}}}"))
                    .build()),
            describe(
                Mutation.newInsertOrUpdateBuilder("user_profiles")
                    .set("key")
                    .to("u3")
                    .set("id")
                    .to("user_profiles/u3")
                    .set("rev")
                    .to("_revu3")
                    .set("doc")
                    .to(Value.json("{\"_key\":\"u3\",\"emoji\":\"\ud83d\ude00\"}"))
                    .build()),
            describe(
                Mutation.newInsertOrUpdateBuilder("knows")
                    .set("key")
                    .to("e1")
                    .set("id")
                    .to("knows/e1")
                    .set("rev")
                    .to("_reve1")
                    .set("from_id")
                    .to("user_profiles/u1")
                    .set("to_id")
                    .to("user_profiles/u2")
                    .set("doc")
                    .to(Value.json("{\"_key\":\"e1\"}"))
                    .build()),
            describe(
                Mutation.newInsertOrUpdateBuilder("knows")
                    .set("key")
                    .to("e2")
                    .set("id")
                    .to("knows/e2")
                    .set("rev")
                    .to("_reve2")
                    .set("from_id")
                    .to("user_profiles/u2")
                    .set("to_id")
                    .to("missing/zz")
                    .set("doc")
                    .to(Value.json("{\"_key\":\"e2\"}"))
                    .build()));
    pipeline.run().waitUntilFinish();
  }

  @Test
  public void testDescribeMarksJsonType() {
    String d =
        describe(Mutation.newInsertOrUpdateBuilder("t").set("doc").to(Value.json("{}")).build());
    assertThat(d).contains("doc:JSON=");
  }
}
