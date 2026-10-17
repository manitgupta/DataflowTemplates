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
package com.google.cloud.teleport.v2.source.gcs.reader;

import static com.google.common.truth.Truth.assertThat;

import com.google.cloud.teleport.v2.reader.io.row.SourceRow;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CustomCoder;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link SourceRowCoders}. */
@RunWith(JUnit4.class)
public class SourceRowCodersTest {

  /** A custom coder standing in for a registered {@code SourceRowCoder}. */
  static final class TestSourceRowCoder extends CustomCoder<SourceRow> {
    private final Coder<SourceRow> delegate = SerializableCoder.of(SourceRow.class);

    @Override
    public void encode(SourceRow value, OutputStream outStream) throws IOException {
      delegate.encode(value, outStream);
    }

    @Override
    public SourceRow decode(InputStream inStream) throws IOException {
      return delegate.decode(inStream);
    }
  }

  @Test
  public void testDefaultIsSerializableCoder() {
    Pipeline p = Pipeline.create(PipelineOptionsFactory.create());
    assertThat(SourceRowCoders.forPipeline(p)).isInstanceOf(SerializableCoder.class);
  }

  @Test
  public void testRegisteredCoderIsUsed() {
    Pipeline p = Pipeline.create(PipelineOptionsFactory.create());
    TestSourceRowCoder coder = new TestSourceRowCoder();
    p.getCoderRegistry().registerCoderForClass(SourceRow.class, coder);
    assertThat(SourceRowCoders.forPipeline(p)).isSameInstanceAs(coder);
  }

  @Test
  @SuppressWarnings({"unchecked", "rawtypes"})
  public void testNonSourceRowCoderIsIgnored() {
    Pipeline p = Pipeline.create(PipelineOptionsFactory.create());
    p.getCoderRegistry().registerCoderForClass(SourceRow.class, (Coder) StringUtf8Coder.of());
    assertThat(SourceRowCoders.forPipeline(p)).isInstanceOf(SerializableCoder.class);
  }
}
