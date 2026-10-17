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

import java.io.Serializable;
import java.util.Objects;

/** Name and type of an ArangoDB collection. */
public final class CollectionInfo implements Serializable {

  private static final long serialVersionUID = 1L;

  private final String sourceName;
  private final CollectionKind kind;

  public CollectionInfo(String sourceName, CollectionKind kind) {
    this.sourceName = Objects.requireNonNull(sourceName, "sourceName");
    this.kind = Objects.requireNonNull(kind, "kind");
  }

  public String sourceName() {
    return sourceName;
  }

  public CollectionKind kind() {
    return kind;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof CollectionInfo)) {
      return false;
    }
    CollectionInfo that = (CollectionInfo) o;
    return sourceName.equals(that.sourceName) && kind == that.kind;
  }

  @Override
  public int hashCode() {
    return Objects.hash(sourceName, kind);
  }

  @Override
  public String toString() {
    return sourceName + "(" + kind + ")";
  }
}
