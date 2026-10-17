/*
 * Copyright (C) 2024 Google LLC
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
package com.google.cloud.teleport.v2.reader;

import com.google.cloud.teleport.v2.reader.io.IoWrapper;
import java.util.List;
import org.apache.beam.sdk.transforms.Wait;

/** Factory to provide the {@link IoWrapper}. */
public interface IoWrapperFactory {

  /**
   * Create an {@link IoWrapper} instance for a list of SourceTables.
   *
   * @param sourceTables
   * @param waitOnSignal
   * @return
   */
  IoWrapper getIOWrapper(List<String> sourceTables, Wait.OnSignal<?> waitOnSignal);

  /**
   * Called once, before the pipeline runs, with every source table the migration will read after
   * table selection. Sources that know their complete table list up front can warn about, or
   * reject, source tables that would silently not be migrated. The default does nothing.
   *
   * @param selectedSourceTables source tables that will be migrated (all levels).
   * @param tablesConfigured whether the user restricted the migration with the {@code tables}
   *     option.
   */
  default void validateSelectedSourceTables(
      List<String> selectedSourceTables, boolean tablesConfigured) {}
}
