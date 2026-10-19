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
package com.google.cloud.teleport.v2.source.gcs.iowrapper;

import com.google.auto.value.AutoValue;
import com.google.cloud.teleport.v2.reader.IoWrapperFactory;
import com.google.cloud.teleport.v2.reader.io.IoWrapper;
import com.google.cloud.teleport.v2.spanner.migrations.exceptions.InvalidOptionsException;
import com.google.cloud.teleport.v2.spanner.migrations.source.config.GcsSourceConfig;
import com.google.cloud.teleport.v2.spanner.migrations.source.config.SourceConnectionConfig;
import java.util.List;
import org.apache.beam.sdk.transforms.Wait;

/** {@link IoWrapperFactory} for the GCS (bulk migration Avro format) source. */
@AutoValue
public abstract class GcsIoWrapperFactory implements IoWrapperFactory {

  /** URI of the root directory of the export, without trailing {@code /}. */
  public abstract String rootPath();

  public static GcsIoWrapperFactory create(String rootPath) {
    return new AutoValue_GcsIoWrapperFactory(new GcsSourceConfig(rootPath).getRootPath());
  }

  /**
   * Creates the factory from the source connection config.
   *
   * @throws IllegalArgumentException unless {@code config} is a {@link GcsSourceConfig}.
   */
  public static GcsIoWrapperFactory fromConfig(SourceConnectionConfig config) {
    if (!(config instanceof GcsSourceConfig)) {
      throw new IllegalArgumentException(
          "Unsupported source connection config type for GCS source: " + config);
    }
    return create(((GcsSourceConfig) config).getRootPath());
  }

  /** Create an {@link IoWrapper} instance for a list of source tables. */
  @Override
  public IoWrapper getIOWrapper(List<String> sourceTables, Wait.OnSignal<?> waitOnSignal) {
    return new GcsIoWrapper(rootPath(), sourceTables, waitOnSignal);
  }

  /**
   * Warns about every export table that will not be migrated (typically: no matching Spanner table)
   * and fails unless the user restricted the table set with the {@code tables} option, since an
   * unmigrated export table is silent data loss.
   *
   * @throws InvalidOptionsException if export tables are not migrated and {@code tables} is not
   *     set.
   */
  @Override
  public void validateSelectedSourceTables(
      List<String> selectedSourceTables, boolean tablesConfigured) {
    GcsIoWrapperHelper.checkAllExportTablesMigrated(
        rootPath(),
        GcsExportDiscovery.listTableFiles(rootPath()).keySet(),
        selectedSourceTables,
        tablesConfigured);
  }
}
