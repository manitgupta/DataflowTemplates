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

import com.google.cloud.teleport.v2.reader.io.row.SourceRow;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.CannotProvideCoderException;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.SerializableCoder;

/**
 * Chooses the {@link Coder} for {@link SourceRow}s: the coder registered in the pipeline's coder
 * registry, if any, else {@link SerializableCoder}.
 */
public final class SourceRowCoders {

  private SourceRowCoders() {}

  /** Returns the coder for {@link SourceRow} to use in the given pipeline. */
  public static Coder<SourceRow> forPipeline(Pipeline pipeline) {
    try {
      Coder<SourceRow> coder = pipeline.getCoderRegistry().getCoder(SourceRow.class);
      Class<?> encoded = coder.getEncodedTypeDescriptor().getRawType();
      if (encoded.isAssignableFrom(SourceRow.class)) {
        return coder;
      }
    } catch (CannotProvideCoderException e) {
      // Fall through to the default.
    }
    return SerializableCoder.of(SourceRow.class);
  }
}
