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
package com.google.cloud.teleport.v2.tools.arangodb;

/** How a collection is split into independent AQL queries. */
public enum PartitioningMode {
  /**
   * Primary-key ranges ({@code d._key >= lo AND d._key < hi}) planned from a key sample. Every
   * range is a primary-index range scan, so all ranges together read the collection once. Default.
   */
  RANGE,
  /**
   * {@code HASH(d._key) % N == i}. Every partition is a full collection scan on the server, so N
   * partitions cost N scans. Only suitable for small collections.
   */
  HASH
}
