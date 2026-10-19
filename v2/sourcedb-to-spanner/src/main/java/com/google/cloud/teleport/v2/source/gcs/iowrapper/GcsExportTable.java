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
import com.google.common.collect.ImmutableList;
import org.apache.avro.Schema;

/** A table of a bulk migration Avro export, as discovered from its files on the launcher. */
@AutoValue
public abstract class GcsExportTable {

  /** Table name: the name of the table's directory under the export root. */
  public abstract String name();

  /** Schema of the {@code payload} record of the table's files. */
  public abstract Schema payloadSchema();

  /** Primary key columns, from the {@code primaryKeys} field of the table's first record. */
  public abstract ImmutableList<String> primaryKeys();

  public static GcsExportTable create(
      String name, Schema payloadSchema, ImmutableList<String> primaryKeys) {
    return new AutoValue_GcsExportTable(name, payloadSchema, primaryKeys);
  }
}
