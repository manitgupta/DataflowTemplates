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
package com.google.cloud.teleport.v2.source.gcs.schema;

import com.google.auto.value.AutoValue;
import java.io.Serializable;

/** Reference to the "schema" of a GCS Avro export, identified by its export id. */
@AutoValue
public abstract class GcsSchemaReference implements Serializable {

  private static final String NAME_PREFIX = "GcsExport.";
  private static final int MAX_NAME_LENGTH = 64;

  /** Export id: the last path segment of the export root directory. */
  public abstract String exportId();

  public static Builder builder() {
    return new AutoValue_GcsSchemaReference.Builder();
  }

  /**
   * Returns a stable unique name to be used in PTransforms: {@code GcsExport.<exportId>} with
   * characters outside {@code [A-Za-z0-9_]} replaced by {@code _}, truncated to 64 characters.
   */
  public String getName() {
    String name = NAME_PREFIX + exportId().replaceAll("[^A-Za-z0-9_]", "_");
    return name.length() > MAX_NAME_LENGTH ? name.substring(0, MAX_NAME_LENGTH) : name;
  }

  /** Builder for {@link GcsSchemaReference}. */
  @AutoValue.Builder
  public abstract static class Builder {
    public abstract Builder setExportId(String value);

    public abstract GcsSchemaReference build();
  }
}
