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

/**
 * A half-open primary key range {@code [lo, hi)}. A {@code null} bound is unbounded, so {@code
 * [null, null)} is the whole collection.
 */
public final class KeyRange implements Serializable {

  private static final long serialVersionUID = 1L;

  /** The whole collection. */
  public static final KeyRange ALL = new KeyRange(null, null);

  private final String lo;
  private final String hi;

  public KeyRange(String lo, String hi) {
    this.lo = lo;
    this.hi = hi;
  }

  /** Inclusive lower bound or {@code null}. */
  public String lo() {
    return lo;
  }

  /** Exclusive upper bound or {@code null}. */
  public String hi() {
    return hi;
  }

  public boolean isAll() {
    return lo == null && hi == null;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof KeyRange)) {
      return false;
    }
    KeyRange that = (KeyRange) o;
    return Objects.equals(lo, that.lo) && Objects.equals(hi, that.hi);
  }

  @Override
  public int hashCode() {
    return Objects.hash(lo, hi);
  }

  @Override
  public String toString() {
    return "[" + (lo == null ? "-inf" : lo) + ", " + (hi == null ? "+inf" : hi) + ")";
  }
}
